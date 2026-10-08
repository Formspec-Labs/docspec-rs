# Source updates: consistency review

**Final static verdict: APPROVE for the source-update checkpoint.** The revised
source closes the blocking retry and old-byte identity findings and the nested
schema mismatch. The new tests directly assert composite-key behavior, history,
write-derived changes, recovery and independent endpoint refusal. No unresolved
consistency blocker remains in this scope. The initial findings below are retained
to explain the changes and their verification.

This is a fresh static semi-formal review of the uncommitted source-update
checkpoint against `2d44f1a`, reviewed on 2026-10-08. The reviewer read the diff,
new write module, updated public types, callers, tests and relevant maintained
Arrow/Iceberg implementations. No tests or benchmarks were executed by this
reviewer. Initial line references preserve the first reviewed diff; any closure
section identifies the revised source separately.

The scope is source updates and their lifecycle records. Independent field-group
backfill, dependent refresh, compaction, retirement and restore are not approved
or treated as implemented by this review.

## Initial findings

### F1 — Concurrent identical updates can return stale-base after their own success

- **Severity:** BLOCKER
- **Category:** correctness
- **Location:** initially `src/engine.rs:736-743`, `806-815`, `854-864`.
- **Evidence chain:** Every pending phase lookup/completion checks `Operation.base`
  against the selected version. Two concurrent calls for the same `UpdateSpec`
  can both begin against base A. The first selects operation B. The second then
  checks a phase or completes preparation and receives `stale_base`, because
  selected B no longer equals A. It does not reach the result reconciliation at
  final selection (`692-696`). The completed-result fast path at request entry
  (`480-484`) cannot handle a result selected after that point.
- **Practical consequence:** One identical request reports success and the other
  reports an apparent conflict even though its exact operation was committed. A
  later retry succeeds, but the immediate API response contradicts the intended
  stable-operation reconciliation behavior.
- **Suggestion:** After an attempt fails, reconcile a durable result only when
  the existing operation's material fingerprint equals the submitted request.
  Preserve strict base refusal for a different operation. Do not weaken the base
  fence or repeat writes to solve this race.
- **Coverage:** The existing source-update test checks competing *different*
  operations (`tests/updates.rs:269-310`), not identical concurrent calls.

### F2 — A rewritten affected file can hide changed base bytes

- **Severity:** BLOCKER
- **Category:** correctness
- **Location:** initially `src/engine/write.rs:103-106`, `182-205`, `295`.
- **Evidence chain:** `open_pin` hashes native metadata; `check_inventory` verifies
  dependency membership, not every dependency's content. Processing then reads
  original Parquet files directly through DataFusion. Unchanged shared files are
  checked against original object identities during candidate pinning. A rewritten
  affected file, however, is absent from the candidate's inventory: its changed
  values can be written into new kept/removed files and receive new valid digests.
  Nothing in the initial preparation path checks its old promised bytes first.
- **Concrete failure sequence:** Publish base A, rewrite one referenced Parquet
  file with valid same-schema/same-count rows but altered values, then update a key
  in that file. The candidate and removed-row outputs can be internally readable
  and correctly hashed while no longer representing A's selected bytes.
- **Suggestion:** Verify the captured base's object identities before deriving new
  files, or use an equally strong qualified immutable-storage guarantee. Add an
  altered-valid-Parquet regression requiring refusal and unchanged latest.
- **Coverage:** The initial source-update tests use intact base files. The earlier
  independent-reader corruption test does not exercise this rewriting path.

### F3 — Nested field metadata is mistaken for a logical schema change

- **Severity:** WARNING
- **Category:** correctness
- **Location:** initially `src/engine/write.rs:334-344`.
- **Evidence chain:** `same_fields` ignores top-level field metadata but compares
  nested `DataType` values directly. Iceberg's maintained conversion adds
  `PARQUET:field_id` to nested fields
  (`iceberg-0.10.1/src/arrow/schema.rs:518-578`). Arrow `Field::eq` includes metadata
  (`arrow-schema-58.4.0/src/field.rs:108-113`). Ordinary replacement Parquet can thus
  have identical logical struct/list names, types and nullability while differing
  only in native field-ID metadata, and receive `schema_mismatch`.
- **Suggestion:** Compare logical schemas recursively or normalize through a
  maintained schema conversion, while continuing to write using the captured
  native schema and its stable IDs. Retain refusal for true nested type or
  nullability changes; do not erase type-bearing extension information blindly.
- **Coverage:** Initial fixtures contain only flat nullable Int64/Utf8 columns.
  Add a nested replacement case and a genuine nested type-mismatch negative case.

### F4 — Composite-key and endpoint guarantees need direct boundary tests

- **Severity:** WARNING
- **Category:** test-coverage
- **Location:** initially `tests/updates.rs:20-52`, `82-105`, `136-149`.
- **Description:** The initial independent reconstruction compares only one
  nullable integer key. It does not establish composite-key matching, quoted field
  names, same-operation concurrency or change-endpoint refusal.
- **Suggestion:** Add composite tuples with partially null components and a quoted
  or mixed-case field name, compare final and removed rows against an independent
  tuple-key oracle, and cover absent-key/no-op updates. Mutate change endpoint
  declarations without changing readable row tables and require verification to
  refuse them. The endpoint fix was already in progress when review began.

## Patch summary

`OperationRequest` unifies full builds and source updates under one operation
record, phase retention and guarded selection path (`src/model.rs:169-229` in the
initial diff). `UpdateSpec` binds a selected base version, typed replacement input,
optional deletion keys and key field names. `ChangePin` retains before/after native
endpoints and the rows actually removed or added. `WriteMetrics` reports observed
write counts rather than computing a later reporting scan.

The new write module finds source files containing affected keys, shares unchanged
files and splits each decoded affected batch into kept and removed rows. New
replacement files are shared between the candidate and added-row table. CLI
`plan-update`/`update` and HTTP `/updates/plan`/`updates/run` call the same engine
operations. The retained release format advances to version 2; this is a greenfield
format change, not a compatibility implementation for earlier development pins.

## Function trace

| Function / method | Initial location | Inputs | Output | Verified behavior |
| --- | --- | --- | --- | --- |
| `OperationRequest` accessors | `src/model.rs:178-229` | Build/update request | Dataset, operation, definition, base and named inputs | Centralizes request identity and sorts named inputs through a BTreeMap. |
| `plan_update`, `plan_request` | `src/engine.rs:155`, `160` | Update/request | RunPlan | Checks request identity, active definition and selected base for unfinished operations. |
| `update`, `update_until` | `src/engine.rs:447`, `457` | Update and optional fault boundary | Release/refusal | Uses the common phase/selection implementation. |
| `run_internal` | `src/engine.rs:462` | OperationRequest | Selected release | Admits inputs, dispatches build/update preparation, records validation/readback and performs guarded selection. F1 applied to its initial form. |
| `fingerprint`, `phase_key` | `src/engine.rs:889`, `836` | Material request dependencies | Stable digests | Update fingerprint includes base, ordered keys, input digests and definition; admission reuse depends on named byte identities. |
| `check_base`, `check_authority` | `src/engine.rs:875`, `854` | Current authority and captured operation | Refusal/success | Exact selected-version comparison joins existing eligibility/definition/epoch checks. |
| `validate_request` | `src/engine.rs:902` | OperationRequest | Refusal/success | Validates digests, nonempty distinct key list and operation/base distinction. |
| `create_output` | `src/engine/write.rs:18` | Dataset, protected directory, native schema | Native table | Uses maintained catalog creation and no-cleanup properties. |
| `process_build` | `src/engine/write.rs:40` | Full-build request and admitted input | Prepared table/metrics | Extracts the existing build logic without changing the common lifecycle. |
| `process_update` | `src/engine/write.rs:85` | Exact-base update and admitted inputs | Prepared candidate, changes and metrics | Requires source-only group; validates input schemas; finds matching files; shares or rewrites; pins native results. |
| `identifier`, `key_match` | `src/engine/write.rs:322`, `325` | Key names and relation aliases | Generated SQL | Escapes quoted identifiers and uses null-safe equality conjunctively across keys. |
| `same_fields` | `src/engine/write.rs:334` | Base and replacement Arrow schemas | Boolean | Requires names/types/nullability in the same order. F3 applied to nested metadata. |
| `any_rows`, `execute` | `src/engine/write.rs:346`, `365` | Maintained query plan | Existence/scanned batches | Existence queries use LIMIT 1 internally; changed-file streams are executed through DataFusion. |
| `split_rows` | `src/engine/write.rs:350` | Joined batch with boolean match marker | Kept and removed batches | Uses Arrow filtering on complementary masks and removes the marker column. |
| `verify_table`, release verification | `src/engine.rs:1066`, `1096` | Exact pins | Verification/refusal | Checks object bytes, native dependency inventory, decoded row count and release/request identities. Initial change checks covered presence/schemas/counts incompletely; closure must inspect revised endpoint validation. |
| Storage `write_batches` | `src/storage.rs:136-144` | Arrow batches | Native files | Newly skips zero-row batches before writer admission, preserving empty-table behavior. |
| CLI / HTTP update handlers | `src/main.rs:171-177`, `src/http.rs:69-80` | Bounded update JSON | Shared engine response | Dispatches plan/update; stale base maps to HTTP conflict. |

## Data flow and invariants

1. **Exact base selection.** A pending update captures `base` in its operation
   record (`src/engine.rs:500-506`). Plan, admission and every durable application
   guard require that version to remain selected (`492`, `743`, `815`, `864`).
   A different update or full build selecting a new version makes the pending
   candidate ineligible. Completed historical retries intentionally bypass current
   base eligibility through their recorded result.
2. **Complete key replacement.** Affected keys are distinct replacement/deletion
   tuples (`src/engine/write.rs:170-179`). Replacement/deletion overlap refuses
   before outputs are written (`139-168`). The `IS NOT DISTINCT FROM` conjunction
   treats each null component as a match (`325-332`). Duplicate old rows are all
   removed for an affected key; duplicate new replacement rows remain separate.
3. **Actual write-derived changes.** A left join against distinct keys produces a
   nonnull boolean match marker (`200-201`). Complementary Arrow masks split a
   single decoded batch into kept/removed rows (`350-362`); the removed stream is
   bounded by a channel while candidate and removal writers run together
   (`233-259`). The actual new replacement files are referenced by both candidate
   and added table (`260-294`). The change rows therefore originate from applied
   writes, rather than a later approximate comparison.
4. **Membership arithmetic.** Prepared count is old rows minus written removed
   rows plus written added rows, using checked arithmetic (`279-288`). This is a
   useful necessary condition, not proof of value-level equality. Independent
   reconstruction tests must verify values and duplicate multiplicity.
5. **Unchanged bytes and history.** Candidate append reuses native descriptors for
   unaffected files. Pinning checks their inherited object identities (`289-305`).
   Old release metadata remains immutable and cleanup is disabled. F2 identified
   why original identities also need checking for files leaving the new inventory.
6. **Recovery reuse.** The captured request fingerprint includes exact base and
   update contents. Input admission excludes locator paths and is reusable by byte
   identity. Completed processing carries candidate, changes and write metrics;
   later phases reuse those exact records. Both input declarations are admitted
   together in the current phase rather than independently checkpointed transfers.
7. **Request/evidence retention.** Updates inherit the base's retained input map and
   add inputs under the update operation's namespace (`src/engine.rs:623-634`). The
   release records its typed request and changes. This preserves selected input
   evidence, but it is not the complete per-value evidence model of the full plan.
8. **Bounded scope.** Bases with multiple groups refuse (`src/engine/write.rs:92-96`)
   so source changes cannot silently leave dependent groups stale. This is an honest
   temporary boundary while R3/R5 remain unfinished.

## Test behavior and edge cases

These are static source assessments, not executed results.

| Test / scenario | Initial location | Expected behavior and evidence |
| --- | --- | --- |
| Applied replacement/deletion with duplicate and null keys | `tests/updates.rs:153` | Final rows match the independent in-memory reconstruction. Removed rows match captured old values; added rows preserve repeated replacements. The test also asserts native-file sharing, observed counters, exact retry, old reads and catalog-free verification. |
| Recovery after durable handoffs | `tests/updates.rs:228` | Each injected post-phase interruption reopens after replacement/deletion source files are removed, compares final rows and asserts retained phase records are unchanged. It simulates host replacement, not process kill inside an update write. |
| Stale different update | `tests/updates.rs:269` | A candidate prepared against initial cannot replace a separately selected update; plan also refuses. Changing the base under the same operation identity conflicts. |
| Overlap, delete-all and append-from-empty | `tests/updates.rs:313` | Null-key replacement/deletion overlap refuses; empty replacement plus complete deletion keys produces an empty result with all old rows recorded as removed; a later replacement inserts into the empty base. |
| Concurrent identical update | No initial test | F1's losing caller can return stale_base after the same operation succeeds. |
| Valid but altered affected base Parquet | No initial test | F2 can re-certify changed bytes unless preparation verifies original object identities. |
| Logical nested schema equality | No initial test | F3 causes false schema mismatch for native nested field metadata. |
| Composite partial-null tuple and quoted field name | No initial test | Conjunctive null-safe SQL appears correct; a tuple oracle is required to substantiate the public behavior. |
| Change endpoint alteration / missing change record | No initial test | Initial verifier is insufficiently strict; implementing agent is adding independent endpoint checks. |
| Empty/absent-key no-op | Partly covered at `:162` | The large mixed update includes an absent deletion key, but no isolated no-op assertion confirms zero actual changes and unchanged values. |

## Exploration notes

- **H1:** Exact-base fencing prevents lost updates. **Confirmed for different
  operations; refined for identical retries:** F1 needs durable-result reconciliation.
- **H2:** Pinning new outputs proves old source identity. **Refuted:** affected files
  drop from candidate inventory, creating F2 unless checked before rewriting.
- **H3:** Replacement/deletion rows are derived from actual writes. **Supported by
  the split-stream and shared added-file trace:** full value qualification remains
  the responsibility of independent reconstruction tests.
- **H4:** Equal nested logical types pass replacement admission. **Refuted in the
  initial code:** native field-ID metadata participates in nested Arrow equality.
- **H5:** Existing tests establish all advertised key forms. **Refuted:** initial
  fixture and oracle support one nullable integer key only.

## Closure inspection

The revised source and assertions were reread on 2026-10-08. Locations in this
section refer to the closure snapshot, while the earlier sections preserve the
initial review evidence. The reviewer did not execute tests or use a green gate
as a substitute for tracing the implementation.

| Finding | Disposition | Revised evidence |
| --- | --- | --- |
| F1: identical concurrent retry | **Closed** | `src/engine.rs:462-491` validates and fingerprints the request before preparation, then checks a durable result after a non-injected failure. It returns that result only when the stored operation fingerprint matches. The ordinary pending-operation guard still requires the exact selected base at `:888-921`. |
| F2: altered affected base bytes | **Closed** | `src/engine/write.rs:103-109` verifies every captured base object before opening the native metadata or registering source files. This covers affected files that later leave the candidate inventory as well as files that remain shared. |
| F3: nested field metadata | **Closed** | `src/engine/write.rs:419-425` compares both schemas after the maintained native conversion assigns comparison IDs. The converter deliberately ignores old field IDs and retains native field names, types and requiredness (`iceberg-0.10.1/src/arrow/schema.rs:296-336`). Output creation still uses the captured native schema (`src/engine/write.rs:279-305`), so comparison does not replace stable output IDs. |
| F4: boundary fixtures | **Closed for this checkpoint** | The revised fixtures directly cover exact tuple matching with partial nulls and escaped names, an empty-key-set no-op, same-operation concurrency, nested values, forged endpoint declarations and HTTP stale-base responses. Their assertions are traced below. |

The composite fixture also exposed a real parser-precedence problem during the
implementer's validation. Closure source parenthesizes each `IS NOT DISTINCT FROM`
expression before joining the expressions with `AND`
(`src/engine/write.rs:410-417`). This makes the intended tuple predicate explicit
to the maintained parser.

### Revised function traces

| Function / method | Location | Inputs and output | Verified behavior |
| --- | --- | --- | --- |
| `run_internal` | `src/engine.rs:462` | Typed request to durable release or refusal | Reconciles its own exact completed operation after a losing attempt; preserves test-injected failures and conflicting identities. |
| `prepare_and_select` | `src/engine.rs:494` | Typed request to selected release | Checks definition/base at entry, records durable phases, verifies candidate/changes, reads back the immutable release and atomically selects authority/result. |
| `phase`, `reserve_attempt`, `complete` | `src/engine.rs:770`, `805`, `840` | Request and captured operation to reusable phase or protected directory | Existing eligibility checks now include the captured base. Phase selection preserves the first completed generation. |
| Update key preparation | `src/engine/write.rs:194-235` | Admitted replacements/deletions to distinct affected-key files | Materializes distinct keys once using the maintained writer. An empty key set registers an explicitly typed empty relation. Both remain within the reserved attempt directory. |
| Update split/write | `src/engine/write.rs:308-342`, `431` | Joined batches to kept/removed native files | Checks the joined batch bound before splitting; sends removed rows through a bounded channel; `try_join!` propagates writer failure without committing a phase. |
| Update append/pin | `src/engine/write.rs:343-403` | Shared, kept, removed and added file descriptors to `Prepared` | Writes replacement files once, shares them with the added table, checks row arithmetic and binds base manifest plus exact native endpoints and field IDs. |
| `check_inventory_files` | `src/storage.rs:456` | Pin and already loaded native inventory to success/refusal | Reuses the existing inventory membership rule without a second manifest traversal. Its wrapper at `:451` still loads an inventory for independent callers. |
| `verify_changes` | `src/engine.rs:1180` | Request, source and optional change pin to success/refusal | Requires changes for updates and absence for builds; verifies bounded base metadata, version/dataset/endpoints, count arithmetic, native key IDs and identical native schemas; independently verifies removed/added tables. |
| `verify_release_with_budget` | `src/engine.rs:1130` | Immutable release to verification result | Checks serialized equality and request/input binding, verifies retained input objects, invokes change verification and verifies every result group. |

The change verifier runs both before validation completion
(`src/engine.rs:646-653`) and during independent release readback (`:1173`). The
processing completion retains the base manifest and both change-table inventories
with the result (`:620-637`). Update inputs inherit prior input evidence and add
operation-namespaced replacement/deletion evidence (`:657-668`). Selection still
occurs only after readback and a fresh authority guard (`:723-753`).

### Revised test assertions and edge cases

These are expected behaviors derived from test source; they are not reviewer-run
test results.

| Test | Location | Assertions and expected behavior |
| --- | --- | --- |
| `applied_updates_preserve_duplicates_null_keys_history_and_unchanged_files` | `tests/updates.rs:179` | Compares final values with a Rust reconstruction independent of generated SQL; checks actual old removed values, exact duplicate replacement rows, shared object identities, observed counters, repeat-operation identity, original historical values and catalog-free verification. |
| `update_handoffs_reuse_completed_work_after_input_loss` | `tests/updates.rs:254` | Interrupts after each durable handoff, deletes submitted input paths, reopens and resumes to the same independently reconstructed values; completed phase records remain byte-for-byte equal as JSON values. |
| `stale_updates_cannot_overwrite_a_selected_result` | `tests/updates.rs:295` | A different selected update makes the prepared candidate and its plan refuse `stale_base`; changing a captured base under the same operation refuses `operation_conflict`. Latest remains the competing selected result. |
| `overlap_refuses_and_empty_replacement_can_delete_every_row` | `tests/updates.rs:339` | Null-key overlap refuses; delete-all produces a readable empty result with the full removal count; a subsequent insert into the empty base succeeds. |
| `identical_competing_updates_reconcile_and_noop_preserves_files` | `tests/updates.rs:379` | Starts identical updates concurrently, requires both to return exactly the same release, and compares unchanged rows, zero added/removed counts and every Parquet object reference. The scheduler determines interleaving; the static error-reconciliation trace establishes the missing race branch. |
| `affected_base_corruption_is_refused_before_rewriting` | `tests/updates.rs:406` | Replaces an old data file with still-valid Parquet containing changed rows, requires `corrupt_object`, and checks latest remains unchanged. This directly exercises old identity checking before candidate rewriting. |
| `paired_writer_refusal_retains_admission_and_can_resume` | `tests/updates.rs:430` | Requires an oversized joined batch to fail within a timeout, leave latest unchanged, retain admission and require processing; after reopening with larger bounds and deleting submitted input paths, the same operation resumes to independently reconstructed values. |
| `composite_nullable_quoted_keys_match_as_tuples` | `tests/updates.rs:471` | Uses mixed-case and escaped-quote names, duplicate tuples, all-null and partial-null keys. Exact final and removed rows are compared with Rust tuple equality. This distinguishes full tuple matching from matching just one component. |
| `independent_verifier_requires_changes_and_exact_endpoints` | `tests/updates.rs:543` | Removes changes, changes native key IDs, replaces the base manifest, or falsifies removed count. It serializes and rehashes each forged release, so `invalid_changes` comes from semantic change checks rather than a trivial outer digest mismatch. |
| `nested_replacements_preserve_values_and_native_field_ids` | `tests/updates.rs:573` | Compares entire native schemas before/after and reads exact struct/list values, including null versus empty lists and null list elements. A changed nested child name refuses `schema_mismatch` and preserves latest. |
| `http_update_routes_share_recovery_and_stale_base_refusals` | `tests/updates.rs:713` | Calls plan/run/repeat-run through the router, then requires a different stale operation to return HTTP 409 and the structured `stale_base` code. |

### Remaining limits

- The independent verifier checks retained byte identities, exact native endpoints,
  schemas, counts and readable change tables. It does not rerun the update as a
  second full production query or prove arbitrary forged same-count row contents.
  Value/multiplicity correctness is supported by the applied-write trace and the
  independent reconstruction fixtures above.
- No timing or ten-million-row performance conclusion follows from this review.
  Updates presently verify old objects and probe source files. Sharing reduces
  rewrites; it does not establish a scan cost proportional to update size.
- The recovery fixtures cover durable boundaries and a bounded writer failure.
  They do not establish power-loss durability inside every write or distributed
  multi-host behavior. Source-only group refusal keeps dependent refresh outside
  this checkpoint (`src/engine/write.rs:92-96`).
- CLI dispatch is a thin shared-engine call (`src/main.rs:171-176`) inspected
  statically. This review did not find a new CLI-process execution fixture; the
  update lifecycle and HTTP behavior have direct test assertions.

## Conclusion

**VERDICT: APPROVE** for this source-update checkpoint.

- The revised patch preserves exact-base authority, derives changes from applied
  writes, retains historical bytes and reconciles stable retries. The concrete
  consistency findings are closed by inspected source and targeted assertions.
- **Coverage of changed paths: ADEQUATE** for the local source-update scope, with
  execution, performance and broader lifecycle limits stated above.
- **Confidence: HIGH** in the reviewed static behavior; no execution claim is made.
- This verdict does not approve dependent field-group refresh, retention/restore,
  hosted operation or completion of the full lifecycle plan.
