# Source-update review: resource use and physical writes

**Current disposition: APPROVE this source-update checkpoint, subject to the
implementation owner's executable gate.** The final closure below supersedes
the initial findings and disposition, preserving their evidence.

Static review on 2026-10-08 of the uncommitted source-update checkpoint relative
to `2d44f1a`. Scope includes `src/engine/write.rs`, changed engine, model,
storage, CLI and HTTP callers, and `tests/updates.rs`. The reviewer applied the
semi-formal-code-review skill and relevant Rust guidelines, read pinned dependency
implementations, and did not run tests or modify implementation files.

## Findings

### UR1 — Recompute distinct affected keys for every base file

**Severity: WARNING. Category: performance. Status: repaired during review.**

The initial `process_update` registered `scope.into_view()` as `affected_keys`,
then queried it separately for every base-file probe and every affected-file
rewrite. DataFusion 53.1.0 stores the logical plan in `into_view`
(`datafusion-53.1.0/src/dataframe/mod.rs:1697`) and constructs its physical plan
again for each scan (`:2666`). It does not materialize this view. Consequently,
the source replacement/deletion scans and distinct-key aggregation recur with
file count, even when the update size stays fixed.

The inspected repair writes the distinct keys once using maintained native
writers (`src/engine/write.rs:181–202`), then reads that prepared result. This
removes repeated input scans and aggregation. It does not eliminate repeated
reads or join construction over the prepared key set; no constant-time update
claim follows. Default DataFusion `cache()` would collect a `MemTable`
(`datafusion-53.1.0/src/dataframe/mod.rs:2387–2402`), so avoiding that unbounded
shortcut is appropriate.

### UR2 — Traverse the same base inventory twice

**Severity: NIT. Category: efficiency / resource admission. Status: repaired.**

The initial path called `snapshot_files`, retained its inventory and reservation,
then called `check_inventory`, which constructed another complete inventory.
This repeated native metadata reads and temporarily reserved another two metadata
allowances for the same result. The repaired caller checks the inventory it
already owns (`src/engine/write.rs:108–109`); `check_inventory_files` shares the
same completeness check with the existing public verification path
(`src/storage.rs:451–473`). This removes duplication without another cache.

### UR3 — Prepared keys use an unconfigured native scanner

**Severity: WARNING. Category: resource correctness. Status: open at inspection.**

Location: `src/engine/write.rs:203–208`.

The newly materialized key table is registered through
`IcebergStaticTableProvider`. Its pinned implementation ignores the DataFusion
session in `scan` (`iceberg-datafusion-0.10.1/src/table/mod.rs:316`) and ignores
the task context in `execute` (`src/physical_plan/scan.rs:144`). Its
`get_batch_stream` builds a default native scan without setting batch size,
concurrency, or holding the admission used by `storage::read_batches`
(`src/physical_plan/scan.rs:211–239`). A caller's small `batch_rows` or
`compute_threads` therefore does not configure this added scan path.

**Suggested repair:** register the prepared Parquet files with DataFusion's
maintained Parquet provider and the shared runtime, supplying an explicit schema
for the empty-key case. This avoids a new custom provider. Preserve preparation
once and the normal batch-size settings; do not claim a whole-process memory
ceiling from those settings alone.

### UR4 — New paired-writer failure path lacks direct coverage

**Severity: WARNING. Category: test coverage. Status: open at inspection.**

Location: `src/engine/write.rs:281–314`; initial `tests/updates.rs:240–277`.

The tests cover successful paired writes and failure after durable handoffs. They
do not yet fail while the new producer/channel/two-writer group is active. The
existing oversized-output test (`tests/lifecycle.rs:628`) exercises a single
build writer. A small update-specific case should exceed the joined batch limit
or exhaust paired-writer admission, verify that the old selection remains, show
that processing is not recorded complete, and resume from retained admission
with a sufficient limit. This checks the materially new failure path rather than
mirroring an internal helper.

**Additional transport gap:** the new `/updates/plan`, `/updates/run`,
`plan-update`, and `update` routes are thin delegates
(`src/http.rs:23–24`, `:69–79`; `src/main.rs:171–175`), but the inspected HTTP test
only posts to `/run` (`tests/lifecycle.rs:549–595`). An update request/conflict
route test would establish the new wire shape. No changed transport behavior is
inferred from the existing build test.

## Patch summary

The checkpoint adds complete replacement rows and deletion keys against an exact
selected source version. It identifies affected files, shares unchanged files,
and writes kept and removed rows from one joined decode. Added rows are written
once and referenced by both the result and added-row journal. The generic engine
flow records the exact input identities, prepared result, journals, validation,
and selected release. It refuses updating a source with dependent groups rather
than selecting stale derived values.

The scope is source updates. Related-dataset refresh, independently computed
field groups, schema backfill, destructive maintenance and the full performance
gate remain outside this checkpoint.

## Function trace

Locations below refer to the working tree with the first UR1/UR2 repairs; later
closure references supersede them when lines move.

| Function / type | File:line | Input → output | Verified behavior |
| --- | --- | --- | --- |
| `UpdateSpec`, `OperationRequest` | `src/model.rs:158`, `:172` | Exact base, typed keys and input digests → request | Reuses the build lifecycle with a typed request; deterministic ordered input declarations bind retained files. |
| `plan_update`, `update`, `update_until` | `src/engine.rs:155`, `:447`, `:457` | Update request → plan or selected release | Delegate to the common request path and its explicit test handoffs. |
| `prepare_and_select` | `src/engine.rs:493` | Request → retained phases and release | Checks identity and exact base before work; admission retains inputs; only completed processing can enter validation and selection. |
| `fingerprint`, `validate_request`, `check_base` | `src/engine.rs:922`, `:935`, `:908` | Material request and selected authority → identity/refusal | Fingerprint includes base, keys, input digests and definition, excludes input paths; duplicate/empty keys refuse; stale bases cannot select. |
| `create_output`, `process_build` | `src/engine/write.rs:18`, `:40` | Dataset, schema, query → native table / pin | Existing build behavior moves to a focused write module and uses the same maintained writer. |
| `process_update` | `src/engine/write.rs:85` | Base release + retained update inputs → candidate and changes | Hashes base objects before reading; checks schema and key overlap; prepares affected keys; probes each file; shares untouched native descriptors. |
| `any_rows`, `key_match`, `same_fields` | `src/engine/write.rs:399`, `:382`, `:391` | Generated SQL, key names, schemas → presence / expression / equality | Presence queries have `LIMIT 1`; quoted identifiers prevent name interpolation; null-safe equality handles null keys; native schema conversion compares nested types and nullability. |
| `split_rows`, `execute` | `src/engine/write.rs:403`, `:419` | Joined batch / DataFrame → paired batches / stream | Splits the final non-null match marker and removes it from stored rows; query execution stays in maintained DataFusion. |
| `write_batches` | `src/storage.rs:109` | Batch stream + native table → file descriptors | Holds four batch allowances through close; skips empty batches; refuses oversized nonempty batches. |
| `snapshot_files`, `check_inventory_files`, `pin` | `src/storage.rs:282`, `:456`, `:384` | Native metadata and inherited identities → closure / exact pin | Reject missing dependencies and unknown outside files; inherited paths retain identity and owner; worker owns inventory reservation through cancellation. |
| `verify_table`, `verify_changes` | `src/engine.rs:1099`, `:1179` | Pins and captured request → verification | Verify payload identities, native membership/readability, endpoint bindings, schemas, field IDs and row-count equation. This is a full-result verification path. |
| CLI / HTTP delegates | `src/main.rs:171`; `src/http.rs:69` | Bounded request metadata → engine result | Route updates through the same engine; stale bases map to HTTP 409. |

Pinned dependency source was read from the local Cargo registry under
`/Users/mikewolfd/.cargo/registry/src/index.crates.io-1949cf8c6b5b557f/`.

## Data flow and invariants

| State or resource | Creation, modification and use | Invariant / practical boundary |
| --- | --- | --- |
| Captured base | `UpdateSpec.base` → operation record → processing → authority recheck (`src/engine.rs:522–538`, `:908`) | A prepared update can select only while its exact base remains selected. |
| Base file identity | Base pin objects → bounded blocking verification before source scans (`src/engine/write.rs:104–109`) | Rewriting a file cannot silently turn a corrupted old payload into a newly hashed valid result. Immutable-file discipline still underlies the subsequent reads. |
| Affected keys | Replacement and deletion keys → distinct relation (`src/engine/write.rs:174–209`) | Overlap refuses; distinct keys prevent join multiplication; duplicate replacement rows remain separate added occurrences. |
| Paired streams | Joined data → byte check → kept/removed split → capacity-one channel (`:281–314`) | The sender is owned by the kept stream; success drops it and lets the removed stream end. An error drops the other `try_join!` future. No processing completion is recorded before both writers and all pins succeed. |
| Admission | Two writer permits plus bounded channel; shared DataFusion runtime (`src/storage.rs:117–151`; `src/engine.rs:99–109`) | Admission refuses rather than waiting while holding another reservation. These controls are not an aggregate resident-memory proof; native decode allocations precede post-decode byte checks. |
| Disk estimate | Affected native file sizes plus retained input sizes and declared scratch (`src/engine/write.rs:238–253`) | Fresh free-space preflight accounts for old files that will be rewritten. It is an estimate, not an output-size guarantee or a reservation against concurrent disk use. |
| Shared files and journals | Old descriptors + new kept/added descriptors → candidate; added descriptors also → added journal (`:345–360`) | Added data is physically written once; unchanged identities are inherited. All new tables live under the existing protected attempt, and cleanup remains disabled. |
| Row counts | Actual native writer counts → checked base minus removed plus added (`:324–343`) | Counts derive from applied rows; unknown deletion keys remove nothing; deleting everything can yield an empty native result. |
| Retry state | Prepared pins and journals → shared phase completion (`src/engine.rs:619–652`) | A failed attempt can leave protected unselected objects; a completed phase is reused without re-running paired writes. |

Review hypotheses: paired writers could deadlock at end-of-input (refuted by
sender ownership and `try_join!` polling); shared added files could escape ownership
(refuted by inherited candidate pins); affected keys were prepared once (initially
refuted, then repaired); configured query settings reached every scanner (refuted
for the newly introduced native provider in UR3).

## Test behavior and concrete edge cases

The following assertions were read, not executed by this reviewer:

- `applied_updates_preserve_duplicates_null_keys_history_and_unchanged_files`
  (`tests/updates.rs:165`) compares the result with plain-Rust full reconstruction,
  checks exact removed and added occurrences, preserves a shared object identity,
  retries the completed operation, reads history, and verifies without the original
  catalog. The traced null-safe distinct-key join is consistent with these checks.
- `update_handoffs_reuse_completed_work_after_input_loss` (`:240`) interrupts after
  each durable boundary, reopens the engine, removes caller input files, resumes,
  and compares previous phase records. This exercises retained-phase recovery,
  not process death during a paired write.
- `stale_updates_cannot_overwrite_a_selected_result` (`:281`) prepares one update,
  selects another, and expects the first to refuse on run and plan. Changing its
  base under the same operation identity expects `operation_conflict`.
- `overlap_refuses_and_empty_replacement_can_delete_every_row` (`:325`) checks
  ambiguous null-key replacement/deletion refusal, deletion of every old row,
  journal counts and native empty readback, then insertion into the empty result.
- Existing native sharing tests in `src/storage.rs` cover inherited owner identity,
  missing inventory references and duplicate manifest accounting. These support
  the reused primitive but do not replace update-specific failure coverage.

Remaining unqualified examples include composite keys with nested payloads,
quoted column names, the `deletions: None` branch, paired-writer interruption,
the new HTTP/CLI wire routes, and sustained metadata growth. They should not be
presented as executed acceptance evidence from the inspected tests.

## Performance and complexity assessment

The physical change is useful: unchanged source files are shared, and actual kept
and removed rows use one joined decode rather than separate transformations.
Maintained Arrow filters, DataFusion queries and Iceberg writers perform the
work. The focused write module avoids duplicating lifecycle selection logic.

The complete update still hashes the base, probes its files, hashes inherited
objects when pinning, and verifies full selected results and journals. The
`files_scanned` counter describes discovery/rewrite passes, not all physical reads
(`src/engine/write.rs:275–278`). Retained inputs and lifecycle metadata also grow
with history (`src/engine.rs:656–666`). This checkpoint cannot establish the
10-million-row gate or update-size-only runtime. Measure payload bytes, metadata
requests, retained history and full readback in the later fixed-update-size gate.

## Initial conclusion

**VERDICT: REQUEST CHANGES** for UR3 and the paired-writer coverage in UR4.

The intended source-update behavior is supported by the static trace and focused
reconstruction tests. Coverage is **INSUFFICIENT** for the newly introduced writer
failure and scanner-control paths. Confidence is **HIGH** for the dependency
trace and resource ownership analysis, and **MEDIUM** for broader query behavior
without execution. No benchmark, aggregate-memory or complete-lifecycle claim is
established by this review.

## Final closure inspection

The repaired source and expanded tests close the actionable findings. These
locations refer to the final inspected working tree and supersede earlier line
references.

- **UR1 closed:** `process_update` writes the distinct key relation once
  (`src/engine/write.rs:202–217`). Later probes read these prepared Parquet files;
  they no longer repeat DISTINCT over the original replacement/deletion inputs.
  The prepared files are intermediate objects under the already protected
  processing attempt. They do not add another independent lifecycle authority.
- **UR2 closed:** the caller passes its existing native inventory to
  `check_inventory_files` (`src/engine/write.rs:108–109`). The shared helper retains
  the completeness check (`src/storage.rs:451–473`); it does not weaken the normal
  verification path. There is one base inventory traversal and reservation here.
- **UR3 closed:** prepared keys now use `SessionContext::read_parquet` with an
  explicit schema (`src/engine/write.rs:227–235`), using the same DataFusion runtime
  and configuration as the other query inputs. There is no new default Iceberg
  scan. When no keys exist, a maintained `EmptyTable` carries the exact schema
  (`:220–226`). The paired no-op test now uses truly empty inputs
  (`tests/updates.rs:379–402`), covering that branch as well as unchanged file
  identities and concurrent identical operations.
- **UR4 closed for the requested failure case:**
  `paired_writer_refusal_retains_admission_and_can_resume`
  (`tests/updates.rs:430–468`) issues a delete-all update under a small batch
  allowance. It asserts both `oversized_batch` and the distinct `joined update
  batch` message, which identifies the pre-split check (`src/engine/write.rs:317–322`)
  inside the paired writer stream. A timeout detects a stalled channel; the test
  checks unchanged selection, reusable admission and incomplete processing, removes
  caller inputs, then resumes and compares against independent reconstruction.
  The assertion distinguishes this path from key preparation or added-row writer
  failures. Arbitrary task cancellation and storage faults remain broader Q1 work.

Additional coverage inspected during closure:

- `http_update_routes_share_recovery_and_stale_base_refusals`
  (`tests/updates.rs:716`) covers planning, execution, retry, HTTP 409 and the
  `stale_base` response body through the new routes. The CLI delegate still has no
  dedicated executable update test; its static trace is direct and unchanged in
  shape from the existing build delegate.
- `composite_nullable_quoted_keys_match_as_tuples` (`:471`) verifies tuple
  equality with mixed null positions, duplicate rows and quoted mixed-case names,
  including exact removed occurrences.
- `nested_replacements_preserve_values_and_native_field_ids` (`:573`) verifies
  nested struct/list values, null versus empty list/string, preserved native field
  IDs, the no-deletions branch and refusal of a renamed nested field. The repaired
  deletion schema comparison uses the maintained schema conversion while allowing
  top-level key nullability (`src/engine/write.rs:142–177`).
- `affected_base_corruption_is_refused_before_rewriting` (`:406`) replaces an old
  file with still-readable Parquet and expects refusal without changing the
  selected release. `independent_verifier_requires_changes_and_exact_endpoints`
  (`:543`) rejects missing change pins, wrong key IDs, substituted base manifests
  and altered removed-row counts even when the outer manifest is rehashed.

The disk preflight includes affected source files and retained update inputs
(`src/engine/write.rs:265–280`). It remains a compressed-byte estimate, not a
reservation or an absolute bound on expanded outputs. Whole-process resident
memory, arbitrary native decoder buffering, large retained histories and
whole-population verification costs remain unqualified. No additional scanner,
database, cache framework or custom writer was introduced to repair these findings.

**VERDICT: APPROVE**, subject to the implementation owner's full executable gate.

Coverage is **ADEQUATE** for the scoped source-update resource and failure paths
by static trace. Confidence is **HIGH** in the fixes and ownership analysis; no
runtime result or performance measurement is claimed by this reviewer. The last
read-only gate-log inspection still showed Clippy errors in new test code; the
implementation owner was notified and must establish the final passing result.
This verdict does not qualify the full incremental lifecycle or its performance
target.
