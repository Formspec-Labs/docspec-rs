# Dependency correction records: independent static review

**Verdict: APPROVE for this record-service checkpoint.** The deterministic memory-admission failure found during review is fixed. No unresolved correctness blocker was found in the bounded scope. Concurrent execution, cold restart and capacity/corrupt-record coverage remain incomplete; this review does not qualify Q4 or the pending adoption integration.

Reviewed on 2026-10-10. Scope: the uncommitted `src/engine/corrections/records.rs` and correction types in `src/model.rs`, against `docs/dependency-corrections.md`, R1/R3 and Q4. The reviewer read repository guidance, the `ms-rust` and `semi-formal-code-review` skills, and the relevant callers and helpers. No Cargo, formatting, build or test commands were run. Engine/adoption/retention integration remains author-owned and is outside this verdict.

Reviewed file SHA-256 values:

- `src/engine/corrections/records.rs`: `72ae826d21b89ad4fbb195d9ee8061c8f6680d0b4a505212e7f4164238087cba`
- `src/model.rs`: `c6af58d36980d57198758739eccd432ab87742e21cd3bd8c2b03c9fd4a9c0727`

## Findings

### F1 — Maximum-sized admission prevented small corrections — resolved

**Severity:** BLOCKER, resolved during review. **Category:** correctness/resource admission.

The initial implementation reserved `max_metadata_bytes * 8` for every new correction. The test configuration supplies 256 MiB of process memory and a 16 MiB metadata allowance (`src/engine/corrections/records.rs:214`). `Budget` gives admission half the process memory (`src/budget.rs:85-94`), so that 128 MiB reservation could never fit alongside the retained request/task and lifecycle-cache reservations (`src/engine/worker.rs:91-97`, `src/engine/worker.rs:123-127`, `src/catalog.rs:215`). Positive tests would fail with `capacity` before the correction commit.

The author changed the reservation to eight times the counted serialized size of the incoming correction and current requirements (`src/engine/corrections/records.rs:67-70`). The counting writer checks the limit without allocating a serialized copy (`src/engine/inputs.rs:485-503`), and the resulting decision/requirements pair receives another size check before encoding (`src/engine/corrections/records.rs:91-95`). This removes the deterministic maximum-size admission failure. This conclusion is static; passing execution remains to be established by the author.

### F2 — Concurrency and restart claims lack direct record-service fixtures

**Severity:** WARNING. **Category:** test-coverage. **Location:** `src/engine/corrections/records.rs:225-323`.

The tests cover sequential cumulative updates, identical/conflicting retry content, a response lost immediately after the native commit, and invalid requests. They do not run distinct corrections simultaneously, reopen the engine before replaying decisions, or exercise metadata-capacity/corrupt-record refusals. The lock and commit traces support the intended behavior, but sequential union does not exercise contention and the lost-response fixture reuses the same engine.

Add focused cases for simultaneous distinct correction IDs whose field sets overlap, cold reopen followed by replay of an older decision after a newer correction, and a bounded refusal that leaves both prior records and global authority unchanged. Assert each returned decision's native revision and the cumulative union. These are qualification gaps, not evidence of a present lost-update defect.

### F3 — Processor identity is selected by the running binary

**Severity:** OBSERVATION. **Category:** maintainability/interface. **Locations:** `src/model.rs:820-838`, `src/engine/corrections/records.rs:21-25`, `src/engine/corrections/records.rs:151-155`.

`DependencyCorrection` has no processor input. The service writes the current `backfill::PROCESSOR`, includes it in the requirements key, and rejects a retained decision whose processor differs from that constant. This provides an exact scope for the currently supported scalar processor. It also means an identical retry after a processor-version change will return `corrupt_metadata`, and the API cannot directly target an older processor's retained definition. Document the current-processor restriction for this checkpoint. Before supporting correction/replay across processor versions, retain an explicit requested processor and resolve historical acknowledgments against their retained identity.

### F4 — Backfill helper visibility — resolved

**Severity:** OBSERVATION, resolved during review. **Category:** interface. **Locations:** `src/engine/backfill.rs:21`, `src/engine/backfill.rs:117`.

The initial module called private sibling-module items. The author exposed `PROCESSOR` and `validate_definition` with `pub(in crate::engine)` and declared the correction module (`src/engine.rs:31`, `src/engine/corrections.rs:3`). This resolves the observed visibility mismatch without widening the public crate API. No compilation claim is made.

## Patch summary

The patch adds a stable correction request and immutable acknowledgment (`src/model.rs:817-838`). `Engine::correct_dependencies` records supplemental source-field requirements for an exact scalar group definition, separately from global eligibility and selected output (`src/engine/corrections/records.rs:42-108`). Current requirements contain the cumulative field set, first/current revisions and latest decision ID; they do not repeat a growing history of decision references (`src/engine/corrections/records.rs:11-17`).

The important in-scope callers are the record-service tests (`src/engine/corrections/records.rs:225`, `:282`, `:302`). The public method delegates to `correct_dependencies_admitted` through existing owned-work admission (`src/engine/corrections/records.rs:46-51`). Root-owned route and adoption callers were being implemented concurrently and are not assessed here.

## Function trace

| Function or method | Location | Inputs and output | Verified behavior |
| --- | --- | --- | --- |
| `scope_key` | `src/engine/corrections/records.rs:21` | Exact `GroupDefinition` → hashed record key | Hashes current scalar processor and full definition, without source definition, operation ID or global eligibility. |
| `decision_key` | `src/engine/corrections/records.rs:29` | Correction `Id` → record key | Gives independently addressable decisions inside the dataset lifecycle. |
| `correct_dependencies` | `src/engine/corrections/records.rs:42` | Request reference → decision/error | Reserves before cloning, then runs the owned service future. |
| `correct_dependencies_admitted` | `src/engine/corrections/records.rs:54` | Owned request reference → retained decision/error | Validates, locks dataset, checks immutable retry, reads current requirements, unions fields, checks bounds, commits both records, reconciles errors with a fresh view. |
| `requirements` | `src/engine/corrections/records.rs:112` | Exact lifecycle view and definition → optional requirements | Checks input size, charges decoding, looks up directly, validates scope, actual row revision, positive revisions and positive nonempty field set. |
| `correction_decision` | `src/engine/corrections/records.rs:139` | Exact view and request → optional decision | Charges decoding, checks processor and native row revision, and refuses changed request content under the retained ID. |
| `validate` | `src/engine/corrections/records.rs:167` | Request → success/refusal | Reuses scalar-definition validation and requires a nonblank reason and positive nonempty source-field IDs. |
| `corrupt` | `src/engine/corrections/records.rs:184` | None → structured refusal | Uses `corrupt_metadata` for scope/revision failures. |
| Test fixture helpers | `src/engine/corrections/records.rs:196`, `:214` | ID/fields or no input → request/limits | Build deterministic scalar requests and explicit memory limits. |
| `request_memory` / `admit_work` | `src/engine/worker.rs:91`, `:111` | Request/future → reservation/owned result | Bound request clones, account for task overhead, and retain normal work under maintenance admission. |
| `Engine::lock` | `src/engine.rs:236` | Dataset → shared mutex | Returns the same lock from the shared engine map for concurrent calls on engine clones. |
| `metadata_size` | `src/engine/inputs.rs:485` | Serializable value and limit → counted bytes | Refuses oversized serialization using a counting sink. |
| `validate_definition` | `src/engine/backfill.rs:117` | Group definition → success/refusal | Rejects source-group identity, missing definition/fields, duplicate IDs/names, nonpositive IDs, reserved names and blank expressions. |
| `SnapshotRows::reserve_decoded` / `get_record` | `src/catalog.rs:74`, `:97` | Record keys → reservation / typed value with row revision | Checks retained size before decoding; returns actual retained row revision independently of serialized content. |
| `Lifecycle::open` / `rows` | `src/catalog.rs:142`, `:203` | Dataset/view → immutable view/cached rows | Uses native sequence number; caches by exact metadata location and bounds retained history. |
| `Lifecycle::commit` | `src/catalog.rs:288` | View and both changes → new view/error | Checks unique keys, increments revision, bounds change/history/cache size, appends atomically through the guarded catalog and publishes the cache only after success. |
| `GuardedCatalog::load_table` / `update_table` | `src/catalog.rs:564`, `:576` | Native table/commit → result/error | Rejects changed metadata location and disables blind retry of failed updates. |
| `record` | `src/catalog.rs:540` | Key and serializable value → JSON record | Encodes the value without writing the catalog. |

## Data flow and invariants

**Stable retry identity.** Validated `Id` values are bounded ASCII identifiers (`src/model.rs:17-44`). The dataset selects a separate native lifecycle (`src/catalog.rs:115-123`); the correction ID then selects its decision key. Full request equality includes dataset, ID, exact definition, field set and reason (`src/model.rs:818-827`, `src/engine/corrections/records.rs:157-162`). An identical retry returns before reading or rewriting current requirements (`src/engine/corrections/records.rs:64-66`), so replay of an earlier correction cannot regress a later cumulative union.

**Cumulative union.** The dataset mutex is held from before the first lifecycle open through commit/reconciliation (`src/engine/corrections/records.rs:61-108`). The existing field set is moved into the next state, then extended with the request set (`src/engine/corrections/records.rs:75-84`). `BTreeSet` supplies deterministic ordering and set union. Same-engine concurrent calls serialize; a native conflict outside that local lock cannot silently write a stale union because commit is guarded. The service returns the error if fresh lookup finds no decision; the caller's retry begins again from current requirements (`src/engine/corrections/records.rs:99-106`).

**Actual revision and lost response.** Both records contain `view.revision + 1` and are passed to the same `Lifecycle::commit` (`src/engine/corrections/records.rs:71-97`). `Lifecycle::commit` uses the same calculation for native row revisions (`src/catalog.rs:306-335`). A successful return must report that revision; a failed return triggers a fresh decision lookup that compares serialized revision with the actual row revision (`src/engine/corrections/records.rs:96-105`, `:148-155`). The fault hook fires after native append and before cache publication (`src/catalog.rs:403-412`), so the test targets the intended uncertain-response boundary.

**Global authority.** The only changed keys are `dependency-correction/<id>` and the hashed requirements key (`src/engine/corrections/records.rs:92-95`). Global invalidation separately writes `authority` and increments its `eligibility` (`src/engine.rs:725-740`). The correction service does not call that method or write `head`; the lifecycle revision still advances, which lets final selection detect a newly committed correction through its existing expected-view guard. This records the necessary authority change; selection enforcement itself is outside scope.

**Bounds and retained size.** Admission bounds the request before cloning, counts request plus current requirements before union, and counts the decision plus final requirements before encoding (`src/engine/corrections/records.rs:46-47`, `:69-70`, `:91`). Lifecycle commit additionally accounts for keys, retained append history and current cache size (`src/catalog.rs:314-320`, `:390-396`). Immutable decision rows remain individually addressable; repeated requirement versions and decisions still consume the existing bounded lifecycle history. No payload scans, descendant traversal, new database or secondary index are introduced by this module.

**Integrity limit.** The requirements reader validates its own shape and native row revision; it does not independently reconstruct cumulative history or cross-check `latest_decision` against the decision row (`src/engine/corrections/records.rs:125-136`). The reviewed producer preserves those relationships by building both records together and committing them atomically. This review assumes the existing guarded lifecycle is the authoritative writer; it does not claim detection of arbitrary coordinated storage tampering.

## Test behavior and edge cases

These are static predictions after the reservation/visibility fixes, not execution results.

| Test | Location | Trace and expected behavior |
| --- | --- | --- |
| `retry_conflict_and_cumulative_corrections_preserve_authority` | `src/engine/corrections/records.rs:226` | Seeds authority/head; expects exact first-decision replay, conflicting-field refusal without a new revision, cumulative `{2, 3}`, stable first revision, latest decision/native revision, old-decision replay, unchanged authority/head and isolation from a changed expression. No logical failure found after F1; no contention is exercised. |
| `lost_commit_response_reconciles_actual_native_generation` | `src/engine/corrections/records.rs:283` | Enables the native post-commit response-loss hook, expects successful reconciliation, decision revision equal to reopened native view, stable retry and matching requirements revision. No logical failure found after F1; it is a fresh view in the same engine, not a host restart. |
| `invalid_requests_leave_native_revision_unchanged` | `src/engine/corrections/records.rs:303` | Tries blank reason, empty fields, zero field ID and blank expression, then asserts unchanged native revision. Each request refuses before correction commit at validation. |

Concrete untested cases are simultaneous corrections to the same scope, cold-restart retry after later corrections, same ID with changed reason/definition, same ID in distinct datasets, checked revision overflow, malformed retained revisions, and cumulative/commit history capacity refusal. Existing branches provide conservative refusal or isolation for these cases, but the above tests do not demonstrate them.

## Review reasoning record

- **H1:** Dataset locking plus guarded native commit prevents lost fields. **Confirmed by static trace:** lock precedes open/read; union uses the read state; both changes commit together; conflict reconciliation never retries that union. Evidence: `src/engine/corrections/records.rs:61-108`, `src/catalog.rs:337-345`, `src/catalog.rs:564-583`. Concurrent execution remains untested here.
- **H2:** Lost-response recovery returns the original native generation. **Confirmed by static trace:** fresh lookup independently checks row revision and exact request. Evidence: `src/engine/corrections/records.rs:99-105`, `:145-163`, `src/catalog.rs:403-412`.
- **H3:** Memory admission scales with actual metadata. **Initially refuted; confirmed after correction:** maximum allowance reservation was replaced with bounded actual-size accounting. Evidence and remaining execution gap are F1.
- **H4:** Scoped correction preserves global authority fields. **Confirmed by write-set inspection and test assertions:** only correction keys are written; authority/head equality is asserted. Evidence: `src/engine/corrections/records.rs:92-95`, `:269-276`.

## Conclusion

**VERDICT: APPROVE** for the bounded static record-service review.

- The reviewed code implements stable same-processor retry identity, cumulative requirements, guarded paired commits and lost-response reconciliation while preserving global eligibility and selected-output records.
- Coverage of changed paths: **INSUFFICIENT for full qualification**; concurrent/cold-restart/boundary cases in F2 remain, and the author must execute the focused tests and required gate.
- Confidence: **HIGH** in the traced record behavior and the identified reservation fix; **MEDIUM** in full integration, which is deliberately outside this review and still under implementation.
