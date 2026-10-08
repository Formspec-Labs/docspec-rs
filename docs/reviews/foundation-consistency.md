# Foundation consistency review

**Final verdict: APPROVE for the documented local foundation.** The implementation
now closes the cache-recovery blocker and the query/planning discrepancies found
below. The original findings are retained as review evidence; see the closure
review and final conclusion for their dispositions. This approval does not qualify
the unfinished full lifecycle or performance target.

This is an independent **static review**, performed on the uncommitted foundation
on 2026-10-07. No tests or benchmarks were executed by this reviewer. Line numbers
refer to the reviewed working-tree version. The review applies the
`semi-formal-code-review` skill and applicable Rust guidelines. Incremental changes,
backfill, retention, restore and hosted publication are documented future scope;
their absence is not presented as a regression in the foundation.

## Initial findings

### F1 — A warm cache can acknowledge history that fails after restart

- **Severity:** BLOCKER
- **Category:** correctness
- **Location:** `src/catalog.rs:149-157`, `src/catalog.rs:249-267`
- **Description:** Warm commits and cold reads enforce different meanings of the
  same metadata limit. Commits bound the size of the latest key/value map; cold
  loading sums every historical decoded batch, including superseded values.
- **Evidence chain:** `Lifecycle::commit` obtains the cached previous map, replaces
  changed keys and checks only `next_values` (`249-254`). It then appends more
  Iceberg rows and installs the next warm cache (`255-267`). After process restart,
  `rows` scans all appended rows and refuses when their cumulative decoded size
  exceeds `max_metadata_bytes` (`149-157`), even if the latest map remains small.
  All `get` and `records` calls depend on that cold load (`104-123`). Consequently,
  repeated successful updates to one key can leave a dataset whose acknowledged
  authority is accessible in the current process but whose `latest`, `inspect`
  and `resume` fail with `metadata_capacity` after reopening. Ordinary engine
  operations repeatedly rewrite the growing `authority` record, so this is not
  limited to unusual direct library use (`src/engine.rs:465-479`, `666-674`).
- **Concrete failure sequence:** Use a small metadata limit; repeatedly commit a
  value substantially smaller than that limit to the same key while retaining
  the warm cache; confirm each commit succeeds; drop `Lifecycle`, construct a new
  instance against the same catalog and call `get` for that key. Cumulative
  history eventually exceeds the cold cap although the live map never did.
- **Suggestion:** Bound peak decoded batch and current-map memory independently
  while streaming historical rows. If a separate scan-work cap is required, make
  admission enforce the same recoverability limit before acknowledging a commit.
  Add a regression that proves every acknowledged update remains readable after
  clearing the cache/reopening under identical limits. A durable index is not
  necessary to fix this inconsistency.
- **Existing coverage:** The guard test checks exact old/new cached views
  (`tests/lifecycle.rs:125-180`); reopen tests use tiny histories
  (`223-252`, `531-598`). Neither crosses the cold cumulative-history boundary.

### F2 — Immutable functions do not establish repeatable relational results

- **Severity:** WARNING
- **Category:** correctness
- **Location:** `src/engine.rs:888-915`
- **Description:** Qualification checks function volatility and the table name,
  but accepts order-sensitive operations without a deterministic order. This is
  broader than the documented absence of a physical output-order promise.
- **Evidence chain:** `validate_plan` walks every expression but only tests its
  function signature's `Volatility` (`898-908`). The pinned DataFusion
  `datafusion-functions-window-53.1.0/src/row_number.rs:79-82` declares
  `row_number` immutable, while `144-151` assigns numbers by encountered row
  position. Therefore `SELECT id, CAST(row_number() OVER () AS BIGINT) AS occurrence
  FROM input` passes this check. `LogicalPlan::Limit` is not checked either, so
  `SELECT * FROM input LIMIT 1` also passes. The execution context uses configurable
  parallel partitions (`src/engine.rs:129-135`); no stable occurrence ordering is
  introduced before executing the stream (`548-552`). Changing encounter order
  changes assigned values or selected membership, not only presentation order.
  The reuse fingerprint has no ordering policy (`816-825`).
- **Concrete risk:** An interrupted, incomplete processing attempt can be rebuilt
  under another partition schedule or compute profile and assign different
  occurrence numbers to the same retained rows. A selected completed phase will
  thereafter be reused as if the transform were repeatable.
- **Suggestion:** Refuse unqualified order-sensitive forms initially, or require
  an explicit, qualified ordering policy with sufficient tie breaking. Add
  admission tests for unordered limits and order-sensitive windows. Do not add a
  general SQL determinism framework merely to support forms the product does not
  yet need. Scheduling-dependent output differences were not executed in this
  static review; the missing admission checks are established directly by code.
- **Existing coverage:** `unstable_functions_do_not_become_reusable_results`
  (`tests/lifecycle.rs:398-419`) covers `random` and `now`, not these operators.

### F3 — Plan can advertise work for a definition that run immediately refuses

- **Severity:** WARNING
- **Category:** correctness
- **Location:** `src/engine.rs:143-195`, `src/engine.rs:440-452`
- **Description:** For a new operation, `plan` does not check that the request's
  definition matches the active definition. `run` does, before admission.
- **Evidence chain:** `plan` checks existing operation identity and only calls
  `check_authority` when there is an existing unfinished operation (`153-164`).
  A new operation using a different definition receives reusable/required phases
  and no refusal. `run_internal` compares the active definition with the submitted
  definition and returns `stale_definition` (`445-449`). Both paths are exposed by
  the CLI (`src/main.rs:161-166`) and HTTP (`src/http.rs:54-65`).
- **Concrete failure sequence:** Run dataset `comments` under `comments-v1`, then
  plan a new operation under `comments-v2` without calling `invalidate`. The plan
  reports work to do, but running the identical request refuses it immediately.
- **Suggestion:** Share the cheap active-definition check between plan and run,
  preserving the intentional ability to retrieve an already completed historical
  operation after invalidation. Test the new-operation mismatch and completed
  historical-operation cases.
- **Existing coverage:** Plan assertions only cover an already completed operation
  (`tests/lifecycle.rs:81-83`); the active-definition discrepancy has no test.

## Patch summary

The foundation adds one Rust crate implementing retained Parquet admission,
DataFusion query processing, native Iceberg writes, durable phase records,
independent local verification, immutable release selection and CLI/HTTP access.
The maintained SQL catalog owns only native catalog bookkeeping. Application
authority is stored in Iceberg rows. The intent and limits are explicit in
`README.md:75-111` and `docs/STATUS.md:11-34`.

The principal application entry is `Engine::run` (`src/engine.rs:407`), called by
CLI run/resume (`src/main.rs:164-166`) and HTTP run (`src/http.rs:60-65`).
Independent read/verification is exposed before opening an engine
(`src/main.rs:105-142`). The public `Lifecycle` API also has direct callers in
the native commit test (`tests/lifecycle.rs:125-180`).

Reviewed source: all `src/*.rs`, `tests/lifecycle.rs`, `Cargo.toml`, toolchain and
build/check configuration, README/status, and the local experiment harnesses and
report. Generated raw experiment artifacts were not independently re-audited;
their reported performance is not a conclusion of this static review.

## Function trace

| Function / method | Location | Inputs | Output | Verified behavior |
| --- | --- | --- | --- | --- |
| `Id::new` / deserialize | `src/model.rs:17`, `43` | Identifier | Validated `Id` | Bounds length and excludes traversal punctuation. |
| `Engine::open`, `with_catalog`, `compose` | `src/engine.rs:70`, `81`, `90` | Root, limits, optional catalog | Shared engine | Acquires local OS lock, canonicalizes root, configures shared memory/spill runtime. |
| `WorkspaceLock::acquire` | `src/budget.rs:98` | Root | Held OS lock | Exclusive ownership persists through the file handle. |
| `Budget::new`, `reserve` | `src/budget.rs:49`, `73` | Limits, byte amount | Admission permit/refusal | Divides capacity from DataFusion; does not claim full RSS control. |
| `Engine::plan` | `src/engine.rs:143` | RunSpec | RunPlan | Resolves recorded reuse without payload reads; F3 applies. |
| `inspect`, `latest`, `release` | `src/engine.rs:202`, `213`, `232` | Dataset/version | Metadata or release | Read one native lifecycle view under the dataset mutex. All depend on cold reconstruction. |
| `invalidate` | `src/engine.rs:352` | Dataset, reason, definition | New eligibility | Preserves selected historical result while atomically advancing eligibility. |
| `run`, `run_until`, `run_internal` | `src/engine.rs:407`, `416`, `420` | Request and optional test handoff | Release | Stable request identity; reserves coverage; prepares outside selection lock; serializes final selection. |
| `phase`, `complete` | `src/engine.rs:691`, `761` | Request, captured operation, phase | Existing/committed phase | Phase lookup rechecks authority; competing attempts adopt the first completed generation. |
| `reserve_attempt` | `src/engine.rs:726` | Operation, purpose | Output directory | Commits protection before creating the directory. No removal actor is implemented. |
| `check`, `check_authority` | `src/engine.rs:680`, `804` | Captured operation and current authority | Authority/refusal | Requires matching definition, eligibility and epoch. |
| `phase_key`, `fingerprint` | `src/engine.rs:791`, `816` | Request/dependencies | Reuse identity | Admission depends only on selected bytes; processing also binds SQL, definition and explicit processor revision. |
| `validate_spec`, `validate_plan` | `src/engine.rs:827`, `888` | Request/SQL plan | Success/refusal | Rejects non-query SQL, unqualified multiset operators, external table names and volatile functions. F2 remains. |
| `Lifecycle::open` | `src/catalog.rs:59` | Dataset ID | Exact native view | Creates the lifecycle table as needed; snapshots supply the revision. |
| `get`, `records`, `rows` | `src/catalog.rs:104`, `117`, `126` | Native view/key | Snapshot values | Cache identity is exact metadata location; latest revision wins. F1 applies. |
| `Lifecycle::commit` | `src/catalog.rs:193` | View, unique changes | New view | Builds next state, writes native data, guards append, exposes cache after success. |
| `GuardedCatalog::load_table`, `update_table` | `src/catalog.rs:325`, `337` | Expected view/native commit | Native table/refusal | Refuses changed metadata identity and suppresses blind retry after inner update errors. |
| `local_catalog`, `table_properties` | `src/storage.rs:38`, `53` | Root | Catalog/properties | Uses maintained SQLite catalog; disables automatic metadata deletion and GC. |
| `write_batches`, `append` | `src/storage.rs:64`, `106` | Native table and Arrow batches | Native data files/table | Holds writer admission, refuses oversized batches, uses native fast append; preserves empty schema-only tables. |
| `digest`, `copy_verified`, `verify_object` | `src/storage.rs:127`, `181`, `157` | Bytes/digest | Identity or refusal | Streams hashing, creates retained copy exclusively, verifies expected input identity. |
| `write_new` | `src/storage.rs:168` | Path and bytes | Immutable local file | Exclusive creation, file sync and parent sync. Wider machine-loss durability remains unqualified. |
| `files_under`, `pin`, `open_pin` | `src/storage.rs:226`, `256`, `276` | Owned directory/table | Exact inventory/native table | Inventories owned files, refuses symlinks, verifies metadata and snapshot/schema IDs without catalog. |
| `verify_table`, `verify_release` | `src/engine.rs:931`, `968` | Pins and budgets | Verification/refusal | Hashes retained objects, decodes table rows, binds manifest/request/input identities. It is not independent re-execution of arbitrary SQL. |
| `compare` | `src/engine.rs:251` | Two releases/group | Schema/input equality and multiset counts | Verifies releases, compares schema exactly and uses signed occurrence counts for duplicates. |
| CLI main / JSON helpers | `src/main.rs:90`, `217`, `225` | Bounded JSON/command | JSON or Arrow IPC | Routes to shared services; catalog-free readers run independently; refuses non-loopback serving. |
| HTTP router / handlers / error mapping | `src/http.rs:19`, `36`, `54` | Bounded local HTTP requests | JSON responses | Calls shared services, applies request size limit, maps known structured application refusals. |

## Data flow and invariants

1. **Stable operation identity.** `fingerprint` binds selected input digest, SQL,
   definition and explicit processor revision (`src/engine.rs:816-825`). The
   operation record is created with authority in one native commit (`455-479`).
   Later requests with the same operation ID but a changed fingerprint refuse
   (`431-432`). An identical completed request returns its original immutable
   result before checking newer authority (`434-438`). This is intentional
   historical reconciliation, not a stale result being newly selected.
2. **Mutable qualification.** `Operation` captures definition, eligibility and
   epoch (`458-464`). Invalidation advances eligibility without rewriting selected
   output (`382-397`). Phase completion, reservations, readback recording and
   selection check current qualification (`633`, `653`, `749`, `770`). A change
   during expensive preparation therefore prevents adoption at the durable handoff.
3. **Coverage cannot move backward.** New requests advance a dataset-local boundary
   (`455-463`). Selection refuses a boundary older than the current selection
   (`654-662`). Routine preparation is allowed to complete while later input has
   been accepted, provided later coverage has not already been selected. Exact
   schema cutover is future scope, not implemented by this routine rule.
4. **Completed generations are retained.** Admission and processing records become
   reusable independently of release selection (`484-583`). `complete` returns an
   already committed winner (`779-780`), and the caller uses that winner. A losing
   candidate's protected files remain retained. This avoids adopting values from
   one attempt while labeling them with another attempt's phase record.
5. **One publication selection.** New release files are exclusively created and
   independently verified before readback completion (`612-639`). Authority and
   completed result are committed together (`664-674`). Readers resolve that
   application selection rather than unrelated native table heads. Readback is
   operation-specific because input coverage may advance while expensive values
   remain unchanged (`603-605`).
6. **Exact object identity.** Expected input digest is checked while copying
   (`src/storage.rs:181-223`). Table pins bind metadata, schema, snapshot, inventory
   and row count (`256-269`); verification checks bytes and decodes native rows
   (`src/engine.rs:931-1011`). Phase lookup itself performs only size checks
   (`713-720`), relying on owned immutable storage and later independent readback.
   This is not a general defense against concurrent arbitrary filesystem mutation.
7. **Native optimistic concurrency.** The reviewed pinned Iceberg source refreshes
   the table before applying actions (`iceberg-0.10.1/src/transaction/mod.rs:218-249`).
   The project guard refuses such a changed base (`src/catalog.rs:325-345`). Native
   append requires the expected branch snapshot (pinned append test at
   `iceberg-0.10.1/src/transaction/append.rs:539-548`), and the SQL catalog applies
   requirements then compares `metadata_location` in the database update
   (`iceberg-catalog-sql-0.10.1/src/catalog.rs:974-1023`). The local profile therefore
   has a concrete refusal path for stale lifecycle decisions. This trace does not
   establish an unqualified remote catalog's semantics.
8. **Cache disposability.** Exact metadata identity prevents a cached snapshot from
   masquerading as another (`src/catalog.rs:127-136`). However, F1 breaks the stronger
   invariant that removing the cache preserves availability under the same limits.

## Test behavior and edge cases

The following assessments describe source traces, not executed results.

| Test | Location | Static assessment and relevant assertion |
| --- | --- | --- |
| `exact_readback_idempotency_and_independent_catalog_free_read` | `tests/lifecycle.rs:70` | Expected to pass for its small fixture: exact retry leaves lifecycle records unchanged; catalog is hidden; native reader checks duplicate count and null/empty distinction. |
| `native_snapshot_guard_rejects_stale_application_state` | `:126` | Expected stale second view refusal; verifies old and current cache views and duplicate-key rejection. Does not inject a race inside catalog update. |
| `live_workspace_owner_and_conflicting_operation_refuse` | `:183` | Expected OS-lock refusal and operation conflict after SQL changes. |
| `bad_input_and_non_query_sql_never_select_a_release` | `:201` | Expected early SQL refusal, then copied input digest mismatch; latest remains absent. |
| `every_durable_phase_survives_reopen_and_source_loss` | `:223` | Expected reuse after each acknowledged handoff; checks retained phase records stay identical after the source is removed. History is too small to reveal F1. |
| `dependency_correction_fences_adoption_without_output_change` | `:256` | Expected stale eligibility after processing; no release is selected and completed processing remains retained. |
| `new_operation_reuses_qualified_values_and_original_generation` | `:286` | Expected original data/input pins with a later boundary and no source file. |
| `older_preparation_cannot_replace_newer_coverage` | `:303` | Expected old completion refusal after a newer query result is selected; latest remains newer. |
| `simultaneous_retries_reconcile_to_one_immutable_release` | `:331` | Expected shared winning phase/readback and equal releases; exercises same-operation contention. |
| `empty_output_keeps_schema_and_readable_history` | `:345` | Expected zero-row snapshot-free table, retained schema and readable old result. |
| `independent_reader_rejects_same_size_corruption` | `:368` | Expected digest refusal after a byte change without size change. |
| `unstable_functions_do_not_become_reusable_results` | `:398` | Expected volatility refusals for clock/random; does not cover F2's order-sensitive immutable functions. |
| `local_http_returns_the_engine_selection_and_structured_conflicts` | `:422` | Expected run/latest JSON equality and conflicting request HTTP 409. |
| `comparison_preserves_duplicates_and_historical_release` | `:472` | Expected one removed duplicate, unchanged input identity and preserved historical result; unqualified `EXCEPT ALL` refuses. |
| `oversized_output_refuses_then_resumes_from_admitted_input` | `:501` | Expected writer byte refusal, retained admission and successful restart with larger limits. |
| `killed_cli_worker_resumes_every_completed_handoff` | `:531` | Harness waits for explicit post-handoff signal, kills the child and resumes without the source. It verifies a row count, not a fresh full semantic oracle at every handoff. |

Concrete uncovered cases are F1's cache-loss boundary, F2's order-sensitive query
forms and F3's new-definition plan. Broader types, provider failures, power loss,
shared retirement, metadata compaction and fresh-catalog restore remain explicitly
unqualified in `docs/STATUS.md:30-56`; no narrow test above proves those guarantees.

## Exploration notes

- **H1:** Native transaction refresh might silently rebase an application decision.
  **Refined:** The project guard rejects changed metadata, and the reviewed native
  snapshot requirement plus SQL compare-and-swap closes the concurrent append path
  for the supported local catalog.
- **H2:** Competing retries might publish different phase generations.
  **Refuted for the implemented path:** Phase completion returns the existing
  winner, and readback/final selection reconcile existing records.
- **H3:** A disposable cache should preserve cold/warm behavior.
  **Refuted:** Cumulative-history versus current-map accounting establishes F1.
- **H4:** Function immutability proves repeatable queries.
  **Refuted:** The maintained row-number implementation supplies a concrete
  counterexample to the admission rule; scheduling-dependent output variation
  still needs execution evidence before any broader performance/correctness claim.
- **H5:** Plan and run share all cheap eligibility checks.
  **Refuted:** A new operation bypasses the definition check only in plan (F3).

## Closure review

The fixes were re-read after implementation. References in this section identify
the updated source; references above preserve the initially reviewed version.

| Finding | Disposition | Current code and test evidence |
| --- | --- | --- |
| F1 | **Closed** | `SnapshotRows` retains cumulative `history_bytes`. Cold replay sums `history_row_bytes` and checks it before accepting rows (`src/catalog.rs:151-181`). Warm commit adds precisely the same measure before native append (`249-270`); the shared measure and refusal are at `285-297`. `every_acknowledged_metadata_update_survives_cache_loss` (`tests/lifecycle.rs:183-224`) cold-reads every successful overwrite, then proves the capacity refusal preserves the last acknowledged value. |
| F2 | **Closed for the identified forms** | The AST visitor refuses windows and limit/offset/fetch before admission (`src/engine.rs:863-889`), and the logical plan independently refuses limits, windows and any aggregate whose maintained `order_sensitivity()` is not `Insensitive` (`918-954`). This also covers the follow-up examples `array_agg` and `first_value`, which are immutable yet order sensitive. The regression (`tests/lifecycle.rs:512-545`) checks windows, nested limits, offset, fetch, array aggregation, first value, random and clock functions, and verifies no release is selected. |
| F3 | **Closed** | `check_definition` is shared by plan and run (`src/engine.rs:163`, `448`, `816-823`). Completed historical operations retain their earlier reconciliation behavior. The regression (`tests/lifecycle.rs:264-294`) checks matching plan/run refusals and verifies the original completed operation remains accessible after the active definition changes. |

The F1 remedy deliberately limits append-only history before accepting unreplayable
state; it does not solve metadata growth. Compaction and larger-history
qualification remain necessary under the full plan. The F2 remedy conservatively
refuses forms that need ordering qualification; it does not establish exact
semantics for every numeric type or aggregate under every execution configuration.
These boundaries belong in the continuing qualification work, not in a new generic
SQL framework for this commit.

The implementing agent reports that the full format, Clippy and test gate passed
after the changes. This reviewer read the added assertions and their application
paths but did not independently execute that gate. The companion resource review
owns the additional blocking-I/O and reader-admission changes; they are not used as
evidence to expand this review's lifecycle conclusions.

## Conclusion

**VERDICT: APPROVE** — scoped to the documented local foundation.

- The reviewed fixes close the identified restart-recovery, query-admission and
  planning discrepancies. No remaining blocker was found in the reviewed local
  lifecycle paths.
- **Coverage of changed paths: ADEQUATE** for these concrete fixes by static test
  inspection; broad lifecycle/provider qualification remains incomplete.
- **Confidence: HIGH** in the direct control-flow and regression-test closure.
  This is static approval, not an independent execution certificate.
- The full user plan remains unfinished for the reasons accurately recorded in
  `docs/STATUS.md`. This review does not approve unimplemented future features or
  establish the complete performance target.
