# Local authorized export review

Current verdict after the delta review: **APPROVE for the local implementation slice.** F1 is resolved; F2 is narrowed by the new tests. No new blocker was found. The initial review below remains a record of the earlier snapshot. This is a static review of the uncommitted export work against `d57042e00a43b821b9730ad688d94ad6d2e011f4`, reviewed on 2026-10-09. The root agent owns execution of the repository gate and tests; this reviewer ran no build, formatting, or tests and changed no implementation.

## Delta review — lookup identity and primitive exports

Static follow-up on 2026-10-09; no execution. The root agent owns focused test results. This delta reviews the requested lookup/type changes and their assertions, not other concurrent writer-resource or restore changes.

- **F1 resolved.** `lookup_export` now loads the retained operation, checks the result against it, and compares the requested key with the exact result ID or saved channel (`src/engine/export.rs:121-135`). `retained_lookup_binds_requested_identity_and_output_is_protected` creates valid output, asserts that retention inspection blocks removal, inserts that valid pin under wrong result/channel keys, requires `invalid_export` for both aliases, and verifies the original ID still resolves (`src/engine/export.rs:881-967`). This directly exercises the reported failure.
- **F2 primitive coverage improved.** `primitive_exports_preserve_types_values_nulls_and_authorized_column_order` covers Boolean, integers, floats, binary/fixed binary, decimal, date, time and zoned/unzoned timestamps, including nulls, empty binary, negative temporal values, integer extrema and deliberately reordered columns (`tests/exports.rs:159-278`, `:324-359`). It reads the actual Parquet independently and compares typed arrays to fixture values by a unique row ID (`:361-388`), avoiding an engine-against-engine oracle.
- **Timestamp normalization preserves values.** The shared native writer derives the expected Arrow schema once (`src/storage.rs:244`) and changes timezone metadata only when source and destination are both zoned timestamps with the same unit (`:323-335`). The cached Arrow 58.4.0 cast implementation clones equal-unit integer values at `arrow-cast-58.4.0/src/cast/mod.rs:1801-1806`, adjusts values only for an unzoned-to-zoned conversion at `:1817-1848`, and then installs the destination type at `:1850`. This call therefore preserves epoch values, units and nulls. Iceberg 0.10.1 maps zoned timestamps to `+00:00` (`iceberg-0.10.1/src/arrow/schema.rs:45`, `:667-676`). The fixture independently sets that expected label with `with_timezone`, preserving original values (`tests/exports.rs:315-323`). General non-UTC-zone admission is not newly qualified by the UTC fixture.
- **Native binary support is corrected.** `authorized_schema` now accepts `LargeBinary` while retaining the explicit primitive-only policy (`src/engine/export.rs:645-659`). This matches Iceberg's binary-to-Arrow mapping (`iceberg-0.10.1/src/arrow/schema.rs:690-691`). The fixture derives expected 64-bit offsets from the original binary values (`tests/exports.rs:312-314`) and still checks bytes/nulls through typed equality. Unsupported-type errors now identify the field and actual type (`src/engine/export.rs:661-666`), which makes refusals actionable without broadening authorization.
- **Footer refusal/retry coverage improved.** `flat_export_footer_limit_preserves_prior_selection_and_can_resume` creates the source under a normal profile, reopens with small output row groups and a restricted metadata allowance, and requires the specific flat-footer refusal (`tests/exports.rs:392-445`). It verifies that the prior channel remains selected, the request remains retained, and no materialization/result was acknowledged (`:446-456`); reopening with sufficient capacity must produce exact values and promote the completed pin (`:459-475`). This tests controlled refusal and retry, not a process-memory measurement.

Remaining qualification gaps from F2 are disk/sample-size refusals, foreign-owner export dependencies and protection of an interrupted export's source. The new protection assertion covers completed output. Full accepted temporal variants, actual peak memory, full-scale performance and remote publication remain outside this delta.

**Delta verdict: APPROVE.** The requested-identity defect is closed, the two native-type corrections match maintained library behavior, and the new assertions address the reported failures. Confidence remains **MEDIUM** because this reviewer performed static analysis only. The initial findings and line references below describe the pre-delta snapshot.

## Initial findings

### F1 — Historical lookup does not bind the requested identity

- **Severity:** WARNING
- **Category:** correctness
- **Location:** `src/engine/export.rs:97-120`
- **Description:** A malformed retained alias can make `export_pin(dataset, A)` return the valid pin for B. Lookup loads the requested key, then loads `export-result/{pin.id}` and compares those values. It does not check that the requested historical key equals `export-result/{pin.id}`. The channel path likewise does not check its key against the retained operation's channel.
- **Evidence chain:** `export_pin` forms the caller's requested key at `src/engine/export.rs:92-94`; `lookup_export` follows the loaded pin's ID at lines 111-120. If `export-result/A` contains B's pin and `export-result/B` contains that same pin, both checks pass. The normal dataset resolver already checks requested identity at `src/engine.rs:495-498`. No supported normal operation was found that creates this malformed alias; this is a fail-closed metadata gap.
- **Suggestion:** Bind historical lookup to the requested operation ID, and channel lookup to the saved operation's channel. Add a regression that places a valid pin under a wrong result/channel key and requires refusal. The root agent accepted this follow-up while the report was being finalized; its eventual correction is outside this reviewed snapshot.

### F2 — Export-specific qualification does not cover all accepted branches

- **Severity:** WARNING
- **Category:** test-coverage
- **Location:** `tests/exports.rs:43-68`, `src/engine/export.rs:630-648`, `src/engine/export.rs:455-475`, `src/engine/retention/inventory.rs:1018-1068`
- **Description:** The export fixture writes `Int64` and `Utf8`, while the implementation also accepts Boolean, floating point, binary, fixed binary, date/time/timestamp and decimal fields. The tests do not exercise export-specific disk/footer/decoded-byte refusal, malformed retained completions, removal protection, or foreign-owner bindings.
- **Evidence chain:** The fixture schema is constructed at `tests/exports.rs:43-59`; standalone equality asserts those two output types at lines 138-152. The wider allowlist is at `src/engine/export.rs:630-648`. The HTTP test invokes restore-point creation after export at `tests/exports.rs:542-564`, so it exercises an inventory traversal, but it does not assert which export objects or source dependencies block removal. The retained-record branches are new at `src/engine/retention/inventory.rs:1018-1068`.
- **Suggestion:** Add a compact primitive-type round-trip case, a retained-lookup corruption case, and a focused retention case that checks both completed output and an interrupted export's source. Keep decoder peak-memory and complete workload qualification explicitly separate from small fixture correctness.

### F3 — Retention and distribution scope remain intentionally narrow

- **Severity:** OBSERVATION
- **Category:** performance / operations
- **Location:** `src/export.rs:7-19`, `src/engine/export.rs:359-501`, `src/engine/retention/retirement.rs:30-65`
- **Description:** Each new operation materializes one complete file for one group and explicit primitive columns. The supported row policy authorizes all active rows. Export operation, materialization, validation and result roots remain retained; the retirement scopes do not retire export records. Stale or duplicate attempts can therefore retain additional protected output.
- **Meaning:** These choices are conservative and do not weaken the reviewed local guarantees. They are not incremental shard reuse, policy-driven export expiry, full native publication, or a storage-amplification qualification. R2 consumer-format freeze and broader R6/R7 acceptance remain open.

## Patch summary

The patch adds an explicit local export policy and immutable Parquet pin in `src/export.rs:7-85`. `Engine::export` captures an exact accepted source, retains work at materialization and validation boundaries, then commits the immutable result and channel selection together (`src/engine/export.rs:126-219`). CLI and HTTP commands call these same services (`src/main.rs:583-595`, `src/http.rs:390-417`). Shared retention inspection recognizes export records and owner creation evidence (`src/engine/retention/inventory.rs:1018-1068`, `src/engine/retention/owners.rs:816-818`).

Review scope includes both new export modules and the complete export integration test file; export-relevant changes in CLI, HTTP, engine module registration, retention inventory/owners, storage readers and shared worker custody; and their behavior-critical callers and callees. The concurrent compaction and restore implementations are separate review assignments.

The requirements used were `docs/PLAN.md` and `docs/requirements/02-incremental-publication.md`, especially explicit byte disclosure, immutable pins, guarded selection, retained expensive work and proportional verification. This review does not qualify remote access, public authentication, real-provider handoff, full consumer coverage or the complete Comments throughput target.

## Function trace

| Function | File:line | Inputs → output | Verified behavior |
| --- | --- | --- | --- |
| `export`, `export_until`, `export_request` | `src/engine/export.rs:56`, `:65`, `:69` | Spec and optional fault boundary → pin | Reserves request memory, validates explicit shape, submits owned work. |
| `latest_export`, `export_pin`, `lookup_export` | `src/engine/export.rs:83`, `:92`, `:97` | Dataset and channel/operation → pin | Dataset lock and retained-result equality; requested-key gap is F1. |
| `export_admitted` | `src/engine/export.rs:126` | Admitted spec → pin | Reuses completed result, materialization and validation; checks authority and channel again under the selection lock. |
| `admit_export` | `src/engine/export.rs:222` | Spec/digest → retained operation | Conflicting retry identity refused; exact source availability rechecked after owner protection; operation and binding committed together. |
| `check_export_authority` | `src/engine/export.rs:290` | View and captured operation → eligibility/refusal | Requires the captured eligibility and epoch to remain current. |
| `check_export_channel` | `src/engine/export.rs:306` | View, expected pin, source revision → eligibility/refusal | Exact expected channel selection and nondecreasing accepted-source revision. |
| `retain_export_record` | `src/engine/export.rs:335` | Operation, record key/value → retained value | Locks the dataset and retains the first completed value across concurrent attempts. |
| `materialize_export` | `src/engine/export.rs:359` | Captured source/policy → file identity, rows, fields | Captures native live files, projects authorized fields, protects destination before writing, batches writes and hashes accepted bytes. |
| `validate_request`, `check_operation`, `validate_operation_record` | `src/engine/export.rs:505`, `:521`, `:538` | Request/operation → validation | One local source; distinct nonempty columns; exact retry digest/source; verified accepted source. |
| `check_materialized`, `check_pin`, `check_result` | `src/engine/export.rs:546`, `:570`, `:589` | Retained completions → validation | Checks policy names/order, ownership, row count, pin shape and request identity. |
| `authorized_schema`, `describe_schema` | `src/engine/export.rs:610`, `:668` | Native schema/policy → indices, clean schema and fields | Refuses internal/nested columns; retains stable IDs/type/nullability; discards arbitrary field/schema metadata. |
| `validate_output` | `src/engine/export.rs:695` | Materialized file → validation receipt | Checks size, bounded footer, exact schema/rows and a small decoded sample; does not hash the file again. |
| `HashingFile::{create,finish,write,flush}` | `src/engine/export.rs:774`, `:782`, `:797`, `:803` | Sequential writer bytes → durable object identity | Exclusive file creation; hashes only accepted writes; synchronizes file and containing directory. |
| `OccurrenceScan::{values,read_projected_file}` | `src/storage/occurrences.rs:236`, `:308` | Exact native inventory and field indices → batch stream | Uses native field-ID projection and the ID-aware transformer; one selected file at a time. |
| `Occurrences::next`, `check_task` | `src/storage/occurrences.rs:133`, `:365` | Native tasks → exact counted batches | Strips transient file field, preserves row count, rejects partial/filtered/delete-bearing tasks. |
| `snapshot_files` | `src/storage.rs:402` | Exact table → retained inventory | Uses current live data membership and refuses current native delete files. |
| `admit_work`, `spawn_work` | `src/engine/worker.rs:111`, `:203` | Engine closure → owned result | Worker retains maintenance admission and custody after caller cancellation. |
| `completion_records`, `protect_releases` | `src/engine.rs:93`, `src/engine/retention/owners.rs:545` | Retained source → claim/binding | Acquires foreign owner protection before consumer lock; writes matching claim/binding. |
| `Inventory::record`, `validate_binding`, `visit_creation` | `src/engine/retention/inventory.rs:927`, `src/engine/retention/owners.rs:233`, `:795` | Export records → protected objects | Registers source/output roots, validates exact consumer bindings, recognizes output creation. |
| CLI export arms and HTTP handlers | `src/main.rs:583`, `src/http.rs:390` | Bounded request → shared Engine result | No second export or selection implementation. |

## Data flow and invariants

1. **Authorization is explicit.** The caller supplies `rows: all` and column names; there is no implicit column wildcard (`src/export.rs:7-40`, `src/engine/export.rs:505-518`). The reader projects by native field ID (`src/storage/occurrences.rs:325-343`). `RecordBatch::try_new` then binds only projected arrays to the clean authorized schema (`src/engine/export.rs:445-446`). The output writer receives no raw source file, original footer or evidence object. Internal names are refused before writing (`src/engine/export.rs:617-621`).

2. **Membership comes from an exact snapshot.** The retained `ReleasePin` is loaded by accepted operation identity and checked against the requested `VersionRef` (`src/engine/export.rs:239-244`). Live native data files determine rows, and native delete files cause explicit refusal (`src/storage.rs:453-461`). Decoded per-file counts and final group count must agree (`src/storage/occurrences.rs:140-170`, `src/engine/export.rs:482-486`). This is not a directory glob.

3. **The retry identity cannot change its request.** The serialized spec digest is retained with the captured source and authority (`src/engine/export.rs:127-130`, `:272-285`). Reusing the operation with different policy/source/expectation is refused (`:521-535`). Completed retries return their original result before attempting channel advancement (`:133-138`), so an old successful retry cannot roll back a newer channel.

4. **Only selection changes visibility.** Materialization and validation are retained independently. The final dataset lock covers a new view, authority and expected-channel rechecks, and one commit containing result plus channel (`src/engine/export.rs:188-210`). The underlying guarded catalog rejects adopting a changed metadata location (`src/catalog.rs:337-341`, `:564-580`). Native transaction retry therefore cannot silently replace application eligibility checks.

5. **Protection precedes effects.** Foreign source owners are protected before the consumer operation is acknowledged (`src/engine/export.rs:245-285`). The destination prefix is retained before directory/file creation (`:384-420`). The shared worker keeps maintenance read admission across processing (`src/engine/worker.rs:111-133`, `:217-228`); normal destructive maintenance cannot overlap an admitted export. Retention roots preserve the captured source and output afterward (`src/engine/retention/inventory.rs:1018-1068`).

6. **Routine checks remain proportional.** The hash is computed during writes (`src/engine/export.rs:797-801`). Validation reads file size/footer/schema/count and a small sample (`:710-759`). Its receipt is reused on retry (`:159-175`); catalog readback compares the serialized selected pin (`:212-218`). Full equality and independent digest verification occur in tests (`tests/exports.rs:113-155`, `:164-174`).

7. **Resource accounting has explicit limits, with stated gaps.** Output processing is sequential by file and batch, with writer admission retained for its lifetime (`src/engine/export.rs:405-431`, `:442-480`). Footer admission estimates accumulate row-group metadata, and validation checks the actual footer length (`:436-475`, `:714-721`). Disk free space is checked before each batch write (`:454-462`). These are code-level admission/refusal mechanisms. The reader can decode compressed pages beyond returned sample rows (`:730-736`), and neither the static trace nor the fixture establishes a whole-process memory or throughput bound.

The review inspected locally cached Iceberg 0.10.1 projection/reader code and Parquet 58.4.0 writer code, matching `Cargo.lock:2487-2488` and `:3278-3279`. Iceberg's pipeline uses the task's field-ID projection and transformer; Parquet's `finish` flushes the writer before returning. No external library behavior was inferred solely from a function name.

## Test behavior and concrete edge cases

These are expected behaviors from the read traces, not reports of test execution.

| Test | File:line | Expected behavior and basis |
| --- | --- | --- |
| `independent_parquet_contains_only_authorized_columns_and_exact_occurrences` | `tests/exports.rs:159` | Should preserve duplicate/null occurrences, requested order and field IDs; repeated request reuses the same file; changed policy with same identity refuses. |
| `replacements_deletions_and_old_downloads_follow_their_exact_snapshots` | `tests/exports.rs:211` | Should read the corrected active population while the old download remains independently readable; completed old retry leaves current channel intact; older new candidate refuses. |
| `empty_snapshot_keeps_standalone_schema_and_explicit_policy` | `tests/exports.rs:272` | Should create valid zero-row Parquet with schema; absent/empty authorization and nonexistent columns refuse. |
| `validation_and_selection_resume_without_opening_the_source_again` | `tests/exports.rs:318` | Should reuse validation with source metadata temporarily withheld; selection-response loss reconciles the original pin. |
| `stale_authority_and_channel_keep_completed_output_without_promotion` | `tests/exports.rs:372` | Should keep materialized bytes while refusing stale expected channel or changed eligibility. |
| `killed_export_workers_resume_without_repeating_completed_work` | `tests/exports.rs:432` | Should resume each acknowledged materialization/validation/selection boundary without completed work markers recurring. The harness actually sends SIGKILL and verifies signal 9 (`tests/support/mod.rs:62-81`). |
| `http_exports_compaction_and_restore_points_use_the_shared_services` | `tests/exports.rs:466` | Should create/read the same export pin through HTTP, return 404 for unknown export, and allow later shared-service operations. |
| `cli_compaction_and_offline_restore_share_the_application_paths` | `tests/exports.rs:569` | Exercises companion CLI integration; does not export a file and adds no direct export-recovery assertion. |
| `schema_authorization_removes_metadata_and_refuses_internal_or_nested_columns` | `src/engine/export.rs:814` | Should strip schema/field canary metadata and refuse internal/nested names. It checks the schema helper, while the integration reader checks actual output field metadata. |

Concrete uncovered cases are F1's malformed aliases; non-Int64/Utf8 accepted primitive fields; footer/disk/sample refusal; foreign-source ownership; and inspection/removal protection for an interrupted export. A failure during unfinished materialization can leave protected partial output and require a new attempt; only acknowledged completed phases are demonstrated reusable. Full native deletes are refused rather than silently omitted. Changed policy creates a new operation/file, so source physical bytes are not reused under a new disclosure policy.

## Conclusion

**VERDICT: APPROVE for the stated local slice, with F1 and F2 follow-ups.**

- The patch implements explicit authorized flat output, exact local source selection, retained completed work and catalog-selected channels without a second mutable object-store authority.
- Coverage of the core local Int64/Utf8, empty, update/delete and acknowledged-recovery paths is **ADEQUATE** by static inspection. Coverage of the full type/refusal/retention matrix is **INSUFFICIENT** for broader qualification.
- Confidence is **MEDIUM**: implementations, callers/callees, assertions and maintained native code were inspected; no tests were executed by this reviewer.
- No throughput, whole-process peak-memory, incremental distribution, remote-provider or full R2/Q8 acceptance claim follows from this review.
