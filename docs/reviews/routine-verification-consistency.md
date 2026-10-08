# Routine verification consistency review

Review date: 2026-10-08. Base: `a944af9`. Scope: the uncommitted routine-verification implementation, including the new `src/storage/inspection.rs`, its callers, tests, and README/plan/status changes. This is an independent static review. The implementation owner runs Cargo; this reviewer did not run Cargo or change implementation files. Later correction review, if any, is recorded below rather than erasing the initial findings.

**Current disposition: APPROVE after corrections.** F1–F3 are resolved by the correction reviewed below. The targeted retained logs support the new behavior. The owner's full repository gate was still running when this certificate closed; this approval does not assert its completion or qualify performance.

## Initial findings

### F1 — Reused physical files still undergo creation inspection

**Severity: WARNING (P2). Category: performance / requirement consistency.**

`Checks::table` originally forwarded every table to `inspect_table` without remembering checked immutable files (`src/engine/verification.rs:40-60`). The inspector opened every live Parquet footer and selected samples from the whole table (`src/storage/inspection.rs:24-69`). This repeats work for inherited unchanged files and can repeat work for one newly written file within the same validation.

Evidence chain: `replace_keys` adds `added_files` to the candidate and also appends them to the added-rows table (`src/engine/write.rs:499-514`). Creation checks the source first (`src/engine.rs:774-782`), then checks the removed/added tables (`src/engine/changes.rs:110-118`). A no-op update preserves the exact old source pin (`src/engine/write.rs:502-505`), but the new validation still inspects it. A schema-only edit similarly passes its inherited source objects to `storage::pin` (`src/engine/schema.rs:135-141`) and then reaches the same inspector. The inspected file population therefore grows with retained data even where no new physical data needs checking.

Retain a request-local record of checked immutable identities and use the exact completed base for creation reuse. Continue checking every table's own snapshot/schema/reference composition and row arithmetic. When explicit diagnostics reuse a physical-file check across tables, also preserve the physical-schema compatibility check for each table; matching bytes alone do not establish that the file belongs to one of the second table's retained schemas. This request follows `AGENTS.md:31-43,96-106`.

Existing update/schema tests prove file sharing and values, but the initial patch's inspection-count test covers a simple build/read/resume only. It does not prove that an update or rename skips previously checked physical files.

### F2 — Exhausting the aggregate sample allowance refuses valid creation

**Severity: WARNING (P2). Category: correctness / resource handling.**

The initial sample loop passes `sample_bytes - result.sampled_bytes` to `strip_file_column` (`src/storage/inspection.rs:94-95`). That helper returns `oversized_batch` when the next batch exceeds the remaining allowance (`src/storage/schema.rs:103-108`). The loop only stops when consumption exactly equals the allowance (`src/storage/inspection.rs:112-113`).

A table can contain several valid batches, each below the writer's per-batch limit (`src/storage.rs:193-205`), whose sampled total exceeds that limit. The second or third sample then refuses creation after its processing phase has completed. The same failure can cross source/change/evidence table boundaries because those tables share `Checks`. This is a sampling-budget decision, not evidence that the produced table is invalid.

Stop or reduce optional sampling when the remaining total cannot accommodate another sample, preserve per-batch admission, and report the actual inspected scope. Add a regression with multiple individually valid wide batches whose combined samples exceed the request allowance. The initial tests cover footer damage and count disagreement, not sample-budget exhaustion.

### F3 — Coordinated scalar groups reset the sample allowance

**Severity: WARNING (P2). Category: resource handling / requirement consistency.**

The operation checks its source/change/evidence tables with one `Checks` (`src/engine.rs:774-791`), then iterates the selected scalar groups through `prepare_group` (`src/engine.rs:823-855`). Each group starts another `Checks::new(Routine)` (`src/engine/backfill/preparation.rs:151-167`). Thus the creation request can sample the configured file/row/byte allowance again for each new group, contrary to `AGENTS.md:111-113`.

Share the request allowance across freshly validated phases, while keeping each group's retained validation completion independent so retries still skip completed work. Explicit `verify_release_at` already shares its `Checks` across the semantic dependency walk (`src/engine.rs:1531-1539`; `src/engine/verification.rs:150-255`). Existing coordinated-group tests inspect compute-file reuse and exact values, but not this aggregate sample bound.

## Patch summary

The patch separates ordinary reads, routine creation checks, and requested deep verification. Native output gets footer/shape/count inspection plus a small native sample; final readback checks the serialized dataset selection. Explicit deep verification retains payload hashes and reconstruction. Normal reads resolve the manifest and stream the selected table. Known immutable objects preserve their digest during shared-file pinning, and cached scalar output descriptors retain identity checks without hashing their full payload again.

The important public entry points are `read_group`, `read_evidence`, `Engine::read_combined`, `Engine::compare`, `verify_release_at`, and the deep-compatible `verify_release`. CLI `verify` selects routine scope unless `--deep` is given (`src/main.rs:153-172`). `DatasetPin.validation` and `VerificationReport` make the distinction visible (`src/model.rs:92-131`). The README and status explicitly disclaim statistical completeness, full-process memory bounds, and measured throughput.

## Function trace

The locations below refer to the initial reviewed implementation and identify stable function names if later corrections move lines.

| Function or path | Location | Inputs and output | Verified behavior |
| --- | --- | --- | --- |
| `run_internal` creation validation | `src/engine.rs:771` | Prepared source, changes, related evidence → retained validation phase | Uses routine table checks; phase reuse bypasses the check. |
| `run_internal` group assembly/readback | `src/engine.rs:821` | Exact base, prepared groups, captured request → immutable release | Checks group composition, writes/hash-binds manifest, reads it independently, compares exact dataset equality, then guards selection. |
| `Checks::{new,composition,table}` | `src/engine/verification.rs:13` | Scope and table → report counters | Routine/deep selection is explicit; composition bypasses payload inspection. Initial state lacked physical-file deduplication. |
| `inspect_table` | `src/storage/inspection.rs:16` | Exact table pin and sample allowance → inspection counts | Checks object size availability, exact native snapshot/schema, referenced inventory, physical schemas, footer row counts, and small native batches. |
| `storage::pin` | `src/storage.rs:442` | Native table, writer count, known immutable objects → `TablePin` | Reconciles live row metadata; preserves exact inherited object identities and hashes only new inventory objects. |
| `storage::check_available` | `src/storage.rs:108` | Object refs → success/refusal | Checks presence/type/length through shared worker-owned admission, without payload hashes. |
| `verify_table` | `src/engine.rs:1491` | Table pin → full integrity result | Hashes its retained object inventory, verifies references, drains native rows, compares exact count. |
| `verify_release` / `verify_release_at` | `src/engine.rs:1518` | Release and requested scope → completion/report | Compatibility wrapper always requests deep scope; routine completion cannot bypass a deep request. |
| `verification::walk` | `src/engine/verification.rs:150` | Release roots → checked semantic dependency graph | Checks root manifest equality, conflicting identities, cycles, and dependency order; shares requested verification scope. |
| `resolve_release` | `src/engine.rs:1592` | Supplied release → resolved immutable selection | Reads the manifest, checks exact dataset equality and completion flag; does not walk inputs or inspect payloads. |
| `verify_local_release` | `src/engine.rs:1606` | Resolved release and scope → result | Preserves request/version/definition/input binding checks, then source/change/group checks. |
| `read_group` / `read_evidence` | `src/engine/read.rs:38,54` | Release and selected relation → stream | Resolves release then opens and streams only the requested table. |
| `Engine::read_combined` | `src/engine/backfill.rs:94` | Release → joined source/group stream | Resolves selection, opens participating tables, and performs the requested combination with final batch admission. |
| `Engine::compare` | `src/engine.rs:349` | Before/after release and group → comparison | Resolves both immutable selections before reading the requested group; no blanket dependency verification. |
| `prepare_group` | `src/engine/backfill/preparation.rs:6` | Captured request/source/definition → retained group | Reuses prepared and validated phases; initial version allocates a fresh sample allowance per validation. |
| `prepare_backfill` composition | `src/engine/backfill.rs:419` | Current compatible selection plus prepared group → candidate release | Checks exact combination, independently reads serialized selection, then rechecks mutable promotion eligibility. |
| `read_completion` | `src/engine/backfill.rs:642` | File completion and schema → native data-file descriptors | Checks output sizes and descriptor hash; verifies exact descriptor paths, lengths, and row count. |
| `verify_groups` / `verify_group_data` | `src/engine/backfill/verification.rs:21,370` | Dataset/group metadata and scope → result | Preserves request, base, parent, definition, field mapping, schema and row count checks; occurrence coverage and value reconstruction run only for deep scope. |
| `verify_changes` | `src/engine/changes.rs:6` | Request, source, delta metadata and scope → result | Always checks endpoints, native key IDs, unchanged schema and count arithmetic; deep mode checks removed membership and multiset reconstruction. |
| `admit_refresh` | `src/engine/refresh.rs:53` | Captured inputs → retained admission | Loads immutable selections and relationships without full input verification. |
| `source_frame` / `join_plan` | `src/engine/refresh.rs:784,480` | Declared transform and selected inputs → DataFusion plan | Processing enforces cardinality. Routine composition derives schema without executing the joined rows. |
| `verify_related_with_inputs` | `src/engine/refresh.rs:635` | Result/evidence plus selected inputs and scope → result | Always checks relationship and schema consistency; deep mode reconstructs results/evidence and checks cardinality on actual occurrence matches. |
| `select_refresh` | `src/engine/refresh.rs:177` | Prepared release and captured operation → selected release | Builds metadata/availability graph, locks participating datasets, rechecks ancestor/input authority and policy, commits exact release. |
| `process_incremental_refresh` / `replace_keys` | `src/engine/incremental.rs:53`; `src/engine/write.rs:281` | Captured immutable base and mutations → new source/deltas | Removed upfront payload hashing; processing still reads required native data, checks inventory, and writes exact applied changes. |

## Data flow and invariants

- **Immutable selection:** manifest digest and size enter through `ObjectRef`; `read_release` checks dataset/version/owner/format (`src/engine/refresh.rs:431-451`). Ordinary resolution compares the complete decoded dataset to the caller's pin. Native metadata hashes and exact snapshot/schema binding still apply when the requested table opens (`src/storage.rs:535-553`). No native table head becomes selection authority.
- **Durable validation:** creation records completion only after checks succeed (`src/engine.rs:771-798`; `src/engine/backfill/preparation.rs:151-175`). Retried processing and readback still use retained phases and current eligibility checks. Reduced checking does not enable expiration or remove protection.
- **Exact changes:** base version/manifest, before/after metadata, key field IDs, row arithmetic, and schema equality remain mandatory (`src/engine/changes.rs:44-94,119-121`). Full removed-row membership and value reconstruction are explicitly deep (`src/engine/changes.rs:138-154`).
- **Exact group composition:** group definitions, input-field bindings, captured request, source metadata, parent relationships and scalar history remain checked (`src/engine/backfill/verification.rs:21-211`). Schema-only selection validates unchanged physical membership and exact native evolution (`src/engine/schema.rs:264-289`).
- **Sample state:** routine diagnostic counters live inside one request-local `Checks`, escape only as `VerificationReport`, and do not satisfy deep verification. Initial creation resets that state between source validation and scalar groups, as F3 describes. Counters describe inspection activity, not unique logical rows or a statistical sample.
- **Resource boundary:** serialized footer length is checked before the maintained parser (`src/storage/inspection.rs:47-54`), but expanded footer metadata can allocate more. The native sample reads one file at a time and drops each stream after its first batch. The pinned Iceberg reader's single-concurrency path lazily flattens tasks (`iceberg-0.10.1/src/arrow/reader/pipeline.rs:64-88`), so it does not drain all later batches. Parquet may still fetch/decode compressed pages or row groups larger than the returned sample. This is disclosed in `docs/STATUS.md` and is not a measured process-memory or I/O guarantee.
- **Costs retained:** manifest/inventory opening remains proportional to selected metadata and file history; explicit routine diagnostics read all live footers. Deep checking intentionally performs full payload work. No throughput or population-independent latency claim follows from the removed calls.

## Test behavior and edge cases

Test judgments here are static assertion traces. The owner-provided gate log is separate execution evidence and does not validate later untested corrections.

| Test | Location | Assertion trace and expected behavior |
| --- | --- | --- |
| `routine_checks_report_scope_and_deep_checks_find_same_size_input_damage` | `tests/lifecycle.rs:75` | Routine reports bounded samples; same-size input damage is tolerated by routine checks but caught by deep hashing; removing historical input does not block the selected source stream. Expected to pass by the new call paths. |
| `routine_checks_reject_damaged_footer_and_declared_counts` | `tests/lifecycle.rs:121` | Rehashed manifest with forged source rows reaches footer-count disagreement; invalid footer tail reaches Parquet failure. Expected to pass. |
| `cli_validates_once_then_reads_and_resumes_without_inspection` | `tests/lifecycle.rs:163` | Test hooks assert one creation inspector call, none for completed resume/read, routine inspector on explicit verify, deep hook on `--deep`. Does not cover per-file duplication or multi-group scope. |
| `empty_streams_keep_schema_and_known_failures_return_structured_errors` | `tests/http_read.rs:162` | Corrupt historical input no longer blocks a committed empty stream; explicit deep diagnostic returns `corrupt_object`. Matches the revised read policy. |
| `applied_updates_preserve_duplicates_null_keys_history_and_unchanged_files` | `tests/updates.rs:184` | Independent expected multiset and delta tables establish preserved update semantics; shared physical objects and old-version reads remain exact; final deep check remains available. |
| `update_handoffs_reuse_completed_work_after_input_loss` | `tests/updates.rs:259` | Interrupts each retained phase, deletes original external inputs, resumes and checks exact output plus unchanged acknowledged records. New routine phases retain the same recovery boundary. |
| `independent_verifier_requires_changes_and_exact_endpoints` | `tests/updates.rs:579` | Forged missing changes/key IDs/base/counts still fail metadata consistency. |
| `native_malformed_group_coverage_and_field_mapping_refuse` | `tests/backfill.rs:630` | Rehashed malformed group values, occurrences or bindings reach explicit deep checks; wrong computed values return `invalid_group_values`. These checks no longer run on ordinary reads by design. |
| `refresh_reuses_unchanged_file_computation_and_selects_all_groups_together` | `tests/refresh.rs:74` | Computes only missing source files per group after recovery; compares full expected values and exact binding/source pins. Does not count sampling work. |
| `independent_refresh_verifier_rejects_missing_groups_inputs_and_false_source_interpretations` | `tests/refresh.rs:261` | Rehashed missing/rebound groups, inputs, parent, boundary or definition fail deep verification's preserved metadata rules. |
| `rename_reorder_keeps_source_and_computed_files_and_old_reads` | `tests/schema.rs:91` | Verifies unchanged object identities/timestamps, exact renamed reads and combined values, deep verification, and refusal of forged field reinterpretation. Initial patch still inspects unchanged payloads, which timestamp checks cannot detect. |
| `rewritten_native_state_cannot_masquerade_as_a_schema_edit` | `tests/schema.rs:353` | Rehashed native property/high-water/reference changes and false boundary fail exact metadata-transition comparison. |

The concrete untested initial edge cases are F1's duplicate/inherited inspection, F2's valid aggregate sample exhaustion, and F3's coordinated-group aggregate allowance. The review does not claim new failures in ordinary immutable resolution, current-authority selection, or explicit deep reconstruction.

## Initial conclusion

**VERDICT: REQUEST CHANGES.**

The patch achieves the principal read-path separation and retains exact metadata/selection guards and explicit deep diagnostics. Creation still repeats work and has an avoidable sampling-induced failure, so it does not yet meet the complete stated scope. Coverage is insufficient for the identified sampling/reuse boundaries. Confidence is high in the static call traces; this review makes no execution or performance claim.

## Correction review

The implementation owner corrected F1–F3. I read the corrected code and targeted test assertions, plus their retained execution logs. The final implementation review found no remaining material issue in this scope.

| Finding | Correction and evidence | Disposition |
| --- | --- | --- |
| F1 | `Checks::trust` seeds a bounded request-local `CheckedFile` map from the exact captured completed base, including selected source/groups/evidence (`src/engine/verification.rs:45-103`; `src/engine.rs:771-778`). `inspect_table` still checks each table's exact native snapshot/schema, inventory and row total; cached physical-file checks require the same `ObjectRef`, native row count, Parquet format and compatible schema history (`src/storage/inspection.rs:51-137`). Newly opened footers retain their actual physical schema fingerprint, so explicit diagnostics can share file checks without accepting a different table interpretation. Routine diagnostics start with an empty map. | Resolved. |
| F2 | Each selected native batch retains the existing per-batch byte check (`src/storage/inspection.rs:158`). The request samples at most three files, giving an explicit total of at most three batch allowances (`src/engine/verification.rs:124-129`). It does not catch and hide a failed oversized decode. README and status disclose this bound and continue to distinguish returned batch bytes from compressed I/O and process memory. | Resolved. |
| F3 | The same mutable `Checks` instance now flows from source/change/evidence validation into every coordinated `prepare_group` (`src/engine.rs:776-798,837-845`; `src/engine/backfill/preparation.rs:10-18,160-170`). A standalone backfill creates one request state and seeds its captured base (`src/engine/backfill.rs:390-401`). Retained per-group validation keys still bypass completed validation. | Resolved by direct call trace; existing coordinated-group recovery/value tests exercise the changed path. |

The correction also addresses two issues found during its review: sample candidates now come from unchecked files instead of positions in the complete inherited population (`src/storage/inspection.rs:130-132`), and the native-format guard precedes any cached-file skip (`src/storage/inspection.rs:76-95`). The sample reader stops after its selected files, rather than planning the rest of the table unnecessarily (`src/storage/inspection.rs:174-177`). Deep reports omit `data_files_checked`, which the deep verifier does not collect, instead of reporting a misleading zero (`src/model.rs:123-125`; `src/engine/verification.rs:20`).

The known-file map has explicit metadata admission. The blocking inspector owns a cloned permit while it owns the map, preserving admission if its caller is canceled (`src/engine/verification.rs:130-134`; `src/storage/inspection.rs:69-71`). Capacity accounting updates per insertion rather than rescanning the map for each file (`src/engine/verification.rs:81-94`; `src/storage/inspection.rs:123-129`). This is a bounded metadata aid, not persisted cache authority.

### Targeted evidence read

- `routine_verification_inspects_shared_change_files_once` independently forms the union of source/added/removed physical paths and requires the report's footer-check count to equal its unique size (`tests/updates.rs:436-463`).
- `update_creation_inspects_only_new_files_without_duplicate_change_checks` runs a separate CLI update, parses actual per-file inspector hooks, requires a nonempty unique list, rejects every old source path, and rejects deep verification (`tests/updates.rs:468-512`). The complete targeted update test file passes in `docs/reviews/routine-verification-shared-files.log`.
- `routine_samples_allow_several_individually_bounded_wide_batches` creates a real multi-file result with independently admitted wide batches. Creation and routine diagnostics succeed; the sample covers three files and uses more than one batch allowance but no more than three (`tests/lifecycle.rs:162-208`). It passes in `docs/reviews/routine-verification-wide-samples-retry.log`.
- The first wide-sample attempt is retained in `docs/reviews/routine-verification-wide-samples.log`. It failed during processing because the fixture retained a larger Arrow backing allocation than its configured allowance; the revised input uses one-row Parquet row groups so it reaches the intended sample boundary. That first failure is not counted as verification evidence for the corrected sampler.
- `docs/reviews/routine-verification-dedup-compile-retry.log` reports successful compilation. The first corrected full gate is retained in `routine-verification-final-check.log`: the one-row native-rename fixture failed the `disk_capacity` preflight with its default scratch allowance. The owner explicitly configured 16 MiB of scratch for that test (`src/engine/backfill.rs:737-745`); production limits and checks remain unchanged. `routine-verification-final-check-retry.log` shows the replacement full gate progressing, but it had not completed at certificate closure. The earlier `routine-verification-check.log` belongs to the pre-correction implementation and does not establish that the final gate passed.

There is no dedicated executed assertion of the aggregate sample counter across multiple scalar groups; the bound follows directly from passing the same state and retaining the single file ceiling, and existing group tests exercise its calls. No throughput measurement, whole-process memory measurement, or representative Comments qualification was performed by this review. Shared-file size availability checks and metadata traversal still run, so the demonstrated improvement is avoiding repeated footer/payload work; update cost is not claimed to be independent of retained file count.

### Final conclusion

**VERDICT: APPROVE.**

The corrected patch implements the requested separation: routine creation checks new material and retains completion; ordinary reads resolve immutable selection and stream their requested data; explicit deep diagnostics retain full integrity and reconstruction. Exact metadata, schema interpretation, group/change references, current selection authority, and recovery phase boundaries remain intact in the reviewed paths. Targeted coverage is adequate for the reported material findings. Confidence is high in the static review and targeted evidence, with final repository-gate completion left to the implementation owner.

### Implementation-owner gate closure

The corrected full `./scripts/check` completed successfully on 2026-10-08, exit 0.
It includes formatting, all-target/all-feature Clippy with warnings denied, and
the full test suite. See `routine-verification-final-check-retry.log`. The earlier
failed scratch-capacity attempt remains retained. This closes the pending gate;
it makes no performance or whole-process resource claim.
