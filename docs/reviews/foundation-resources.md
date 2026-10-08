# Foundation review: resources, native adapters, and independent reading

**Current disposition: APPROVE the reviewed foundation resource fixes.** The
original findings below are retained as review history; the final closure section
records their resolution. Full lifecycle and performance qualification remain open.

Review date: 2026-10-07. Scope: the uncommitted foundation in `docspec-rs`, before
its initial commit. This is an independent **static review** using the
semi-formal-code-review skill and applicable Rust guidelines. No tests, benchmarks,
or implementation changes were executed by this reviewer. Locations below describe
the reviewed source; later fixes may move them.

## Findings

### F1 — Successful warm commits can make the authority unreadable after restart

**Severity: BLOCKER. Category: correctness.**

Location: `src/catalog.rs:149–157`, `src/catalog.rs:249–267`.

Cold loading accumulates the decoded bytes of *every historical row* and refuses
once that sum exceeds `max_metadata_bytes`. Warm commits instead clone the current
key/value map, replace each changed key, and check only that current map's size.
They then append another historical version to the native table and install the
new cache entry. Consequently, repeatedly overwriting an existing key with a value
that fits the limit can keep returning success while the native history exceeds
the cold-reader limit. Restarting the process, or accessing another dataset and
evicting the single cache entry, makes the acknowledged state return
`metadata_capacity`. `latest`, `inspect`, `plan`, and `run` all depend on this reader
(`src/engine.rs:143`, `202`, `213`, `420`).

This is more than the documented linear history cost: disposing of an optimization
changes whether committed authority can be read under the same limits. The
snapshot cache must not become necessary for access to previously accepted state.

**Suggested fix:** apply the same capacity rule to cold and warm state. A bounded
streaming fold can account for live retained keys and per-batch memory without
treating already released historical batches as resident memory. Alternatively,
refuse a commit before native publication if its resulting history cannot reopen.
Add a small-limit test that overwrites one key repeatedly, checks the final value
with the warm cache, then checks it through a new `Lifecycle` and after switching
datasets. No existing test reaches this boundary; the history probe stops below it
and restarts after each operation (`experiments/local-reuse/history.py:29–49`).

### F2 — Native reader concurrency bypasses the configured admission model

**Severity: WARNING. Category: performance / resource correctness.**

Location: `src/engine.rs:931–950`, `src/catalog.rs:149`, `src/main.rs:131–138`.

`verify_table` reserves two maximum-size batches, but its Iceberg scan leaves
file and manifest concurrency unset. Lifecycle loading and the final CLI export
also use default native scans. In the pinned dependency, `TableScanBuilder::new`
sets all these concurrency limits to available CPU parallelism
(`iceberg-0.10.1/src/scan/mod.rs:66–79`), and the Arrow reader concurrently opens and
flattens those file streams (`iceberg-0.10.1/src/arrow/reader/pipeline.rs:64–84`).
`compute_threads` is applied only to DataFusion (`src/engine.rs:129–135`). A profile
set to one compute thread can therefore still have native readers working across
all available cores and retaining multiple files' decode state while reserving
only two batches. Concurrent `run` requests multiply this mismatch. The CLI's
second scan has no retained `Budget` reservation or per-batch byte check at all;
the budget used by `verify_release` has already been dropped.

The README correctly leaves whole-process RSS qualification open
(`README.md:100–105`), so this finding does not claim that a qualified RSS guarantee
was broken. It identifies a configurable adapter omission that must be repaired
before the resource controls can be qualified.

**Suggested fix:** centralize native scan construction, explicitly set concurrency
from admitted reader capacity, and hold the associated reservation through stream
consumption, including CLI export. Use the maintained scan settings rather than a
custom scheduler. Add a multi-file reader case with low declared capacity and
concurrent readers. The existing oversized-output test (`tests/lifecycle.rs:501`)
tests the writer's post-decode refusal, not this native reader path.

### F3 — Large synchronous file operations can stall the local HTTP execution host

**Severity: WARNING. Category: performance / availability.**

Location: `src/engine.rs:500–505`, `555–560`, `933–935`, `1005–1006`.

The HTTP handler awaits `Engine::run` on Tokio (`src/http.rs:60–64`). That future
directly calls synchronous whole-file copy/hash routines. `copy_verified` loops on
`std::fs::File::read` and `write_all` without yielding (`src/storage.rs:181–210`);
`pin` hashes every written file (`src/storage.rs:256–261`); verification similarly
hashes each complete object using `digest` (`src/storage.rs:127–140`, `157–165`).
These calls hold the executor worker until an entire input or object inventory has
finished. With enough concurrent large runs to occupy the workers, metadata
requests and cancellation/shutdown work cannot advance until a file pass finishes.
Even one large run blocks unrelated tasks in an embedded current-thread runtime.
The documented usage explicitly encourages concurrent requests to one host
(`README.md:69–73`).

**Suggested fix:** move bounded copy/hash work onto Tokio's maintained blocking
facility, with an explicit admission limit, or use chunked asynchronous I/O with
yield points. Keep protection and phase commit decisions in the application
service. A scheduler framework is unnecessary. Add a deterministic host
responsiveness test during a deliberately stalled or bounded slow file operation;
current tests cover only small local files.

## Patch summary

The patch establishes a single Rust crate for retained Parquet input, native
Iceberg output, recoverable phase records, catalog-selected immutable local
releases, and CLI/loopback HTTP access. It uses maintained SQL-catalog, writer,
reader, and query-engine primitives. The intended first slice and unfinished full
lifecycle are distinguished in `README.md:7–10` and `docs/STATUS.md:11–57`.

Important entry points are `Engine::run` (`src/engine.rs:407`),
`Engine::plan` (`143`), `Engine::compare` (`251`), `Lifecycle::commit`
(`src/catalog.rs:193`), `verify_release` (`src/engine.rs:968`), the CLI dispatch
(`src/main.rs:100`), and HTTP `run` (`src/http.rs:60`). The review read all Rust
modules, package/build configuration, the integration suite, both experiment
harnesses and protocols, retained result summaries, and selected raw timing/check
logs. It did not inspect every generated command log individually, and did not
audit all transitive dependencies.

## Function trace

| Function / method | File:line | Inputs | Output | Verified static behavior |
| --- | --- | --- | --- | --- |
| `Id::new` / deserialization | `src/model.rs:17`, `43` | Identifier text | Validated ID | Rejects empty, overlong, traversal and non-ASCII identifier values. |
| `Budget::new`, `reserve` | `src/budget.rs:49`, `73` | Limits / byte request | Shared semaphore / held permit | Gives application admission half the memory allowance; refuses unavailable permits without waiting. |
| `WorkspaceLock::acquire` | `src/budget.rs:98` | Workspace path | OS-held file lock | Prevents another execution host using that path; no timestamp-based ownership guess. |
| `Engine::open`, `with_catalog`, `compose` | `src/engine.rs:70`, `81`, `90` | Root, limits, optional catalog | Shared engine | Holds workspace lock, injects catalog, creates shared DataFusion spill/memory runtime. |
| `Engine::lock`, `context` | `src/engine.rs:119`, `129` | Dataset / limits | Per-dataset lock / query context | Serializes short state transitions; shares native query memory and scratch pool. |
| `Lifecycle::open` | `src/catalog.rs:59` | Dataset ID | Immutable native view | Opens or creates a dataset authority; even read entry points can create its table. |
| `Lifecycle::get`, `records`, `rows` | `src/catalog.rs:104`, `117`, `126` | Exact view and optional key | Decoded state | Cache keyed by metadata location; newest row revision wins. Cold and warm capacity rules differ (F1). |
| `Lifecycle::commit` | `src/catalog.rs:193` | Captured view and key updates | New native view | Rejects empty/duplicate keys, encodes Arrow batch, prepares next cache, appends through guard, then installs cache. |
| `GuardedCatalog::load_table`, `update_table` | `src/catalog.rs:325`, `337` | Table / native commit | Native table | Rejects different metadata location before native transaction refresh; marks underlying update failures non-retryable. |
| `record`, `check_cache_size`, `strings` | `src/catalog.rs:301`, `282`, `308` | Serializable value / map / Arrow batch | Record / limit check / typed column | Serialization is centralized; map accounting is approximate; wrong Arrow string type refuses. |
| `local_catalog`, `table_properties` | `src/storage.rs:38`, `53` | Root | Maintained catalog / settings | SQLite contains native catalog bookkeeping; application rows remain in Iceberg; cleanup flags disabled. |
| `write_batches`, `append` | `src/storage.rs:64`, `106` | Table, Arrow stream, budget | Native data files / snapshot | Reserves writer capacity for its lifetime; refuses oversized produced batches; delegates append to native transaction. |
| `digest`, `object`, `verify_object` | `src/storage.rs:127`, `143`, `157` | Local path / expected object | BLAKE3 identity / refusal | Bounded synchronous read buffer, exact bytes and size, whole-file blocking execution (F3). |
| `write_new`, `copy_verified` | `src/storage.rs:168`, `181` | New path / selected source digest | Immutable local bytes | Uses create-new, syncs file and parent; failed copies stay in already protected attempt directory. |
| `files_under`, `pin`, `open_pin` | `src/storage.rs:226`, `256`, `276` | Owned directory / native table / pin | Inventory / exact static table | Rejects symlinks, bounds entries, hashes owned files, checks metadata digest/snapshot/schema without catalog. |
| `plan`, `inspect`, `latest`, `release` | `src/engine.rs:143`, `202`, `213`, `232` | Spec / dataset / version | Bounded metadata | Read one captured authority and use completed result records, not independent native data heads. |
| `compare` | `src/engine.rs:251` | Two releases, group name | Schema/input flags and occurrence differences | Verifies inputs, refuses absent groups, compares equal schemas with signed grouped counts preserving duplicate differences. |
| `invalidate` | `src/engine.rs:352` | Dataset, reason, optional definition | New eligibility | Retains reason and changes eligibility atomically without replacing selected release. |
| `run`, `run_until`, `run_internal` | `src/engine.rs:407`, `416`, `420` | Run spec / optional test stop | Release pin | Captures operation boundary; retains admission, processing, validation, readback; reconciles completed attempts; refuses coverage regression. |
| `phase`, `reserve_attempt`, `complete` | `src/engine.rs:691`, `726`, `761` | Operation and phase | Reused record / protected path / completion | Checks eligibility, records protection before writes, preserves the winning completion generation. |
| `check`, `check_authority`, `fingerprint` | `src/engine.rs:680`, `804`, `816` | View / operation / request | Eligibility check / digest | Checks definition, eligibility and epoch; material digest excludes retry ID and input locator. |
| `validate_spec`, `SqlQualification`, `validate_plan` | `src/engine.rs:827`, `857`, `888` | SQL/spec/logical plan | Refusal or acceptance | Limits to a single query, rejects unsupported multiset operations and nonimmutable functions, restricts table scans to input. |
| `verify_table`, `verify_release_with_budget`, `verify_release` | `src/engine.rs:931`, `973`, `968` | Exact release/table and limits | Verified identity/readability | Checks digests, manifest equality and bindings, schema/snapshot, and decoded row count. Does not recompute SQL semantics. |
| CLI `Read` / `read_json` | `src/main.rs:110`, `217` | Release pin / output / metadata file | Arrow IPC / parsed metadata | Verifies, then separately streams native data; second reader bypasses shared admission (F2); input JSON limited by file size. |
| `http::router`, handlers, `HttpError` | `src/http.rs:19`, `54–88`, `36` | Bounded JSON or path IDs | JSON / status | Calls engine services directly; 1 MiB request body cap; maps known conflicts/capacity refusals. |
| `Error::refusal`, conversions, display | `src/error.rs:16`, `55`, `42` | Local or upstream error | Stable code with retained source | Preserves upstream causes; library errors retain broad subsystem codes. |
| Probe `command`, `independent_check`, `main` | `experiments/local-reuse/run.py:37`, `62`, `76` | Frozen inputs / binary | Raw artifacts and result flags | Validates retained source hashes, times subprocesses, compares source Parquet with CLI-exported Arrow IPC using PyArrow. |
| History probe loop | `experiments/local-reuse/history.py:29` | Fixed data, increasing operation history | Timing/storage series | Starts a fresh CLI for each operation and checks unchanged input/native pins. Does not stress a long-lived warm cache past its byte limit. |

Native behavior was checked in the locally installed pinned source under
`/Users/mikewolfd/.cargo/registry/src/index.crates.io-1949cf8c6b5b557f/`:

- `iceberg-0.10.1/src/transaction/mod.rs:218–249` refreshes the base before applying
  actions; the wrapper's stale-location refusal therefore addresses a real behavior.
- `iceberg-catalog-sql-0.10.1/src/catalog.rs:974–1023` loads current metadata,
  applies native commit requirements, writes staged metadata, then compares and
  swaps `metadata_location`; zero updated rows produce a retryable conflict.
- `iceberg-0.10.1/src/writer/file_writer/rolling_writer.rs:186–238` rolls native
  files and accumulates their descriptors. The implementation reuses this writer
  rather than defining a second Parquet format.
- `arrow-ipc-58.4.0/src/writer.rs:1462–1474` flushes on `StreamWriter::finish`;
  a suspected silent buffered-export flush loss was refuted.

## Data flow and invariants

| State | Creation, changes and use | Property and failure boundary |
| --- | --- | --- |
| Operation identity | `fingerprint` → admission record → every phase/selection (`src/engine.rs:422–480`, `791–825`) | Same operation cannot bind a different material request. Processor semantic changes require updating `PROCESSOR`; runtime lock digest is separate provenance. |
| Authority | Loaded from one view, modified under dataset lock, appended through guarded catalog (`src/engine.rs:425–480`, `644–674`) | Definition/eligibility/epoch guard adoption; selected input boundary cannot regress. |
| Snapshot cache | Created by `rows`, replaced after successful `commit`, shared by one `Lifecycle` (`src/catalog.rs:126–186`, `249–267`) | Exact metadata identity prevents stale values being mistaken for a different snapshot. Disposable-cache availability equivalence is broken by F1. |
| Application budget | Created with runtime, cloned across lifecycle/processing, held by permits (`src/budget.rs:49–85`; `src/storage.rs:74`) | Reservations share a pool, but do not yet cover all native reader fan-out or pre-decode allocation (F2 and documented Q1 gap). |
| Protected attempts | Catalog row committed before directory creation and payload writing (`src/engine.rs:726–758`) | Failed preparation remains retained. Cleanup is disabled, so no active removal race is claimed solved. |
| Input bytes | Expected digest → protected copy → reusable admission → dataset pin (`src/engine.rs:484–516`, `587–602`) | Original locator may disappear; retained bytes and selected digest remain. |
| Native output | Query stream → maintained writer → snapshot → object pin → phase record (`src/engine.rs:519–570`) | Processing output remains distinct from published authority; failed selection does not destroy it. |
| Release | Pinned dataset → immutable manifest → readback → guarded selection (`src/engine.rs:591–674`) | Retry returns the same completed release. Public/portable output and per-value evidence remain outside this local slice. |

The review began with three hypotheses: the cache would be semantically disposable
(refuted by F1); native reader reservations would cover configured concurrent work
(refined to F2 after reading the dependency); and catalog-free reads would bind the
selected objects rather than current native heads (confirmed for engine-produced
local pins by `open_pin` and manifest verification). A separate hypothesis that
Arrow export discarded final buffered write errors was refuted by dependency source.

## Test behavior and edge cases

The retained gate log reports passing executions
(`experiments/local-reuse/check.log:19–37`). This review checked their assertions
statically; it does not assert a new test run or prove coverage beyond those paths.

| Test and location | Static trace and expected behavior |
| --- | --- |
| `exact_readback_idempotency_and_independent_catalog_free_read`, `tests/lifecycle.rs:70` | Same operation returns its saved result without new records; hiding SQLite still allows static-table reading. Checks duplicate and null/empty values. Consistent with recorded pass. |
| `native_snapshot_guard_rejects_stale_application_state`, `:126` | Stale view cannot append; old exact view remains readable; duplicate keys refuse. Does not cross metadata capacity or inject a race inside the underlying catalog update. |
| `live_workspace_owner_and_conflicting_operation_refuse`, `:183` | Second owner refuses; changed SQL under same operation refuses. |
| `bad_input_and_non_query_sql_never_select_a_release`, `:201` | DDL refuses before work; digest mismatch prevents phase completion and release selection. |
| `every_durable_phase_survives_reopen_and_source_loss`, `:223` | Reopens after each declared handoff with external source removed; compares existing phase records exactly. Small history does not expose F1. |
| `dependency_correction_fences_adoption_without_output_change`, `:256` | Invalidation increments eligibility; old prepared operation refuses; completed processing remains recorded. |
| `new_operation_reuses_qualified_values_and_original_generation`, `:286` | New operation retains the same native/input pins while advancing boundary. |
| `older_preparation_cannot_replace_newer_coverage`, `:303` | Later selected boundary makes older selection refuse; current release remains unchanged. |
| `simultaneous_retries_reconcile_to_one_immutable_release`, `:331` | Two matching requests adopt winning phase/release; validates final release. Small single input does not qualify general resource concurrency. |
| `empty_output_keeps_schema_and_readable_history`, `:345` | Empty output has no data snapshot but retains schema and zero count; previous release verifies. |
| `independent_reader_rejects_same_size_corruption`, `:368` | Alters native Parquet bytes in place; exact object digest rejects it before decode. |
| `unstable_functions_do_not_become_reusable_results`, `:398` | Clock/random functions fail logical-plan volatility validation; no result selected. |
| `local_http_returns_the_engine_selection_and_structured_conflicts`, `:422` | HTTP run/latest agree and conflicting operation maps to 409; no concurrent latency or stalled-I/O assertion. |
| `comparison_preserves_duplicates_and_historical_release`, `:472` | Removing one duplicate yields one removal; historical release preserved; unsupported `EXCEPT ALL` refused. |
| `oversized_output_refuses_then_resumes_from_admitted_input`, `:501` | Writer checks oversized decoded output; larger profile resumes retained input. Does not establish a peak allocation ceiling. |
| `killed_cli_worker_resumes_every_completed_handoff`, `:531` | Child waits after acknowledged handoff, is killed, and new process resumes without external input. Does not exercise power loss or mid-commit unknown outcome. |

Concrete uncovered cases are restart/cache eviction after cumulative history exceeds
the limit (F1, high risk), native multi-file reads under a small admitted pool (F2,
medium risk), and metadata-host responsiveness while large file operations block
(F3, medium risk). These require tests before the corresponding guarantees can be
claimed. The remainder of R1–R7 and Q1–Q8 is explicitly incomplete in
`docs/STATUS.md`; missing future functionality is not mislabeled as a regression
in this foundation review.

## Performance evidence and scope observations

The measured cache intervention is a proportionate response to a demonstrated
problem. Its frozen protocol retained the failed baseline and limited its claim
to the local history range (`experiments/local-reuse/cache-protocol.md`). Raw
operation-50 metrics report 5.502907791 seconds before and 1.228741750 seconds
after; the latter raw time log records 72,138,752 bytes peak RSS. These agree with
the bounded result in `experiments/local-reuse/REPORT.md`. They do not demonstrate
F1 safety, since every measured operation starts a new process and the tested
history never reaches the cold limit.

The PyArrow oracle checks source Parquet against the CLI's exported Arrow IPC
(`experiments/local-reuse/run.py:62–73`). That is useful independent value checking
at the CLI boundary. It does not directly read the produced native Parquet with
a second implementation: the native Iceberg decode still occurs inside the Rust
CLI (`src/main.rs:122–138`). Before claiming independent native artifact
interoperability, add direct external Parquet/Iceberg reading. This is a qualification
scope observation, not evidence that the current artifacts are incorrect.

An unchanged new operation deliberately hashes and decodes data again for its new
release readback (`src/engine.rs:630`, `973–1009`), and a CLI read verifies then
decodes it again (`src/main.rs:116–138`). Keep these full-data passes visible in
future byte/request counters before extrapolating small-input reuse timings. The
current evidence supports saved processing and file reuse, not constant-time
unchanged work on arbitrarily large payloads.

The one-crate structure, maintained native commit wrapper, and feature-gated test
hooks fit the request to avoid unnecessary complexity. None of these findings
requires introducing another SQL ledger, generalized plugin framework, or custom
execution engine.

## Conclusion

**VERDICT: REQUEST CHANGES.**

- The foundation implements a coherent, reviewable local lifecycle slice, with
  useful qualification evidence and explicit limits on its claims.
- F1 breaks recovery under unchanged configured limits after successful commits.
  Repair it before treating the snapshot cache as disposable.
- F2 and F3 identify concrete native-adapter and executor omissions to resolve
  before large/concurrent qualification.
- Coverage of changed paths: **INSUFFICIENT** for the identified history and
  concurrency boundaries; useful and targeted for the small retained-input path.
- Confidence: **HIGH** for the code traces and F1; **HIGH** for the unconfigured
  native concurrency and blocking calls, with their magnitude requiring measurement.

## Follow-up inspection during remediation

The following is a static inspection of subsequent edits, preserving the original
findings and their original locations above. It is not a new test execution.

- **F1 resolved in the inspected code.** Cold replay now accumulates
  `history_row_bytes` (`src/catalog.rs:178–179`), and the warm path computes the
  resulting cumulative size and refuses before append (`src/catalog.rs:264–285`).
  Both use the same helper (`src/catalog.rs:300–312`).
  `every_acknowledged_metadata_update_survives_cache_loss`
  (`tests/lifecycle.rs:183–224`) reconstructs a new lifecycle reader after every
  successful write and after the eventual capacity refusal, asserting the last
  acknowledged value remains readable. This covers the reported counterexample.
- **F2 partly resolved.** Lifecycle replay now reserves decode capacity and sets
  native concurrency to one and batch rows explicitly (`src/catalog.rs:151–168`).
  Verification does the same (`src/engine.rs:944–965`). CLI export now sets
  concurrency and checks decoded batch size (`src/main.rs:130–143`), but still
  needs to hold a reader reservation through that second scan after the standalone
  verifier's budget ends.
- **F3 remains open** in this follow-up snapshot: the application still directly
  calls the synchronous whole-file routines from its async operation path.

## Final resource-remediation inspection

The second follow-up replaces the partial F2/F3 dispositions above. It remains
static review; the implementation owner is running the updated gate separately.

- **F1 remains resolved.** Both warm admission and cold replay use cumulative
  serialized-history accounting (`src/catalog.rs:163–164`, `250–258`, `285–298`).
  This deliberately refuses new work before exhausting readable history; it does
  not claim that the eventual metadata-growth requirement is solved.
- **F2 resolved for the reviewed native-reader call sites.**
  `storage::read_batches` (`src/storage.rs:68–95`) acquires a reader reservation
  before creating the native scan, selects concurrency one and explicit batch
  rows, and carries the permit in the stream state until termination or drop.
  It also refuses oversized decoded batches. Lifecycle replay
  (`src/catalog.rs:151`), verification (`src/engine.rs:972`), and CLI export
  (`src/main.rs:130–136`) all use this function. The CLI now creates its own reader
  budget after standalone verification and retains the stream through writing.
  `native_readers_hold_shared_admission_until_stream_drop`
  (`tests/lifecycle.rs:227–261`) holds the entire application pool with one stream,
  checks a competing read refuses, drops the first stream, then confirms another
  reader can decode the table. This addresses the reported reservation omission;
  decoder allocations before a batch refusal remain part of unqualified Q1.
- **F3 resolved for large payload copy/hash operations.** `Budget::blocking`
  (`src/budget.rs:72–95`) uses Tokio's blocking facility with a shared job permit
  and one-MiB buffer admission held *inside* the worker. Input copy and output
  inventory hashing call it (`src/engine.rs:502–505`, `557–560`), while object-list
  verification goes through `storage::verify_objects` (`src/storage.rs:98–107`;
  callers `src/engine.rs:968`, `1028`). The current-thread test
  `canceled_blocking_work_keeps_admission_without_stalling_the_executor`
  (`src/budget.rs:148–178`) stalls a worker while the sole executor thread advances,
  cancels its caller, proves its admission stays held, and then proves it is
  released on worker exit. Small metadata and filesystem calls remain synchronous;
  this review does not extend closure to a claim of universal nonblocking I/O.

One follow-up edge case was also repaired: the new job semaphore now validates
`compute_threads <= Semaphore::MAX_PERMITS` before calling `Semaphore::new`
(`src/budget.rs:50–68`). Oversized user-supplied limits therefore return
`invalid_budget` rather than triggering the panic documented by pinned Tokio
(`tokio-1.53.2/src/sync/semaphore.rs:479–485`). The focused assertion is
`unrepresentable_worker_limit_refuses_without_panicking`
(`src/budget.rs:149–159`); adding it moved the cancellation test to
`src/budget.rs:162`.

### Closure verdict

**VERDICT: APPROVE** for the reviewed foundation resource fixes, subject to the
owner's normal executable gate. No actionable regression remains in the inspected
fixes. F1, F2 and F3 are resolved by code paths and focused assertions directly
matching the reported scenarios. Coverage of those specific repair paths is
**ADEQUATE** by static inspection; confidence is **HIGH**. This verdict neither
claims a newly executed test run nor closes the broader Q1–Q8, native-interoperability,
and complete-lifecycle qualification work recorded above.
