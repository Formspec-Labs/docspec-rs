# Shared object I/O and Engine pin integration: revised review

2026-10-10. **APPROVE the scoped storage and local Engine integration.** The two
material findings from the first review are resolved. No new blocking finding.
Remote Engine startup and full remote publication remain outside this change.

Static review of `/tmp/docspec-shared-object-io.patch` (SHA-256
`fbafbdc2f49b64ecaaf16b4f3e90b3825dd45c7e8de9e33e87c48f92c5467d28`) and
`/tmp/docspec-engine-object-io.patch` (SHA-256
`276cd62f820e26226a475be4cbb1a58175f75b2a418574c484cd7d754c7a7484`), followed by
inspection of the applied/formatted worktree and its corrected native Table
imports. References below use current repository paths and lines. I ran no
builds, tests, formatting, or provider calls and changed no source files.

## Findings and resolved findings

**F1 resolved — one explicit backend for native inventory and identities.**
`Objects::bind_table` consumes metadata/identifier/selected URI/read-only state,
not the original Table's FileIO (`src/storage/object_io.rs:20-30`). The remote
constructor validates owned scope and canonical metadata placement, then builds
with `objects.file_io()` and retains those same RemoteObjects in the private
binding (`:151-186`). `pin_bound` accepts that binding instead of independent
Table/backend arguments (`src/storage.rs:705-713`). The populated fixture
intentionally supplies a different endpoint on the original Table and asserts
that it receives no requests (`remote/objects/tests/pinning.rs:118-173`).

The native metadata itself remains trusted input from candidate construction or
catalog acknowledgment, as in the previous implementation; this is not a new
arbitrary-metadata integrity-verification API. Local binding clones the original
Table, preserving its runtime, cache, metadata and I/O (`object_io.rs:143-147`).
Remote binding explicitly begins a new current-runtime lifetime (`:173-181`);
it does not claim to preserve a hidden runtime/cache or third-party credentials.

**F2 resolved — reject escaped references before native reads.**
`PinPaths::check` applies configured read scope to every reference and candidate
containment to new references (`object_io.rs:204-209`). The existing inventory
walk invokes it on selected metadata, statistics, lists, manifests and live data
before the corresponding read/insertion (`src/storage.rs:576-665`). The guard is
assembled before that walk (`:726-736`). Known references preserve owner/content
identity and still require configured read authority. The new populated escape
fixture covers both new and inherited out-of-scope lists/manifests and in-scope
but unprotected paths; its assertions inspect rejected URI requests
(`tests/pinning.rs:219-313`).

**Resource observation resolved.** `pin_bound` now reserves its inventory
allowance before cloning captured identities (`storage.rs:715`), then transfers
that same permit to `snapshot_files_guarded` (`:739`). The ordinary inventory
wrapper reserves before its walk as well (`:573-574`). The helper consumes the
permit instead of reserving again (`:577-582`); cancellation still retains it in
the resulting inventory/identity worker. This correction restores early capacity
refusal without duplicate admission or another accounting abstraction.

## Patch summary and function trace

The change introduces concrete private object dispatch, shares the existing
streamed identity reader, guards remote inventory access, and routes production
Engine pin creation through one injected backend.

| Function / caller | Location | Verified behavior |
|---|---|---|
| Engine constructors / `compose` | `src/engine.rs:192-254` | Both public constructors explicitly choose Local; `Inner` owns the choice. No remote startup is exposed. |
| `Engine::pin_output` | `src/engine/write.rs:71-79` | Bind native table, then call shared `pin_bound` using the existing lifecycle budget and shared identities. |
| Engine pin callers | `write.rs:223,439,682-692`; `incremental.rs:93,396,501`; `incremental/history.rs:90`; `schema.rs:191`; `evidence.rs:193`; `compaction.rs:501`; `backfill/preparation.rs:297` | Preserve row counts, captured shared-object lists and original budgets. No write/rebuild/scan is added. |
| `CandidateWriter::create_bytes` / `bind_candidate` | `src/storage/tables.rs:123-145` | Create-only dispatch uses Objects; acknowledged remote candidates bind to explicit configured I/O. Local returned Table is unchanged. |
| `register_candidate` | `tables.rs:148-176` | Existing admitted worker and exact metadata write precede catalog registration; binding afterward adds no I/O. Real remote caller remains `RemoteStorage::register_candidate`. |
| `Objects` identity/availability/create helpers | `src/storage/object_io.rs:34-102` | Concrete Local/Remote calls reuse maintained I/O; no scheduler, registry, or nested admission. |
| `snapshot_files_guarded` / wrapper | `src/storage.rs:572-665` | Same inventory, native decoders and limits; optional concrete path guard. Existing non-pin inventory consumers retain their previous wrapper. |
| `pin_bound` | `storage.rs:705-799` | Reject conflicting identities, inventory and count rows, then reuse captured references or hash new objects once. |
| `verify`, `identity_admitted`, `check_available_admitted`, `identity_by_key` | `src/storage/remote/objects.rs:293,327,344,362` | Existing expected-digest verification and new identity capture share one stream loop; inherited availability performs HEAD only. |

All production `storage::pin` calls in the changed Engine paths migrate. The
remaining local wrapper is test-only; other remaining callers are unit tests.
The remote backend has a real candidate-registration caller, so its construction
does not depend on unused public demonstration helpers.

## Data flow, lifecycle and performance invariants

- Captured identities remain exact `ObjectRef` values keyed by path. Conflicting
  owner/content bindings now refuse before inventory I/O (`storage.rs:715-723`).
  Inherited paths skip hashing and keep their original owner (`:770-776`). Scope
  permission does not replace the caller's lifecycle protection grant.
- The identity worker owns both SnapshotFiles' metadata permit and its read
  allowance through actual completion (`storage.rs:759-768`). The unchanged
  `Budget::blocking_admitted` owns its worker semaphore in the spawned closure
  (`src/budget.rs:147-165`). No detached I/O is introduced outside that ownership.
- Local canonical containment, final-component symlink refusal, regular-file
  availability and maintained BLAKE3 reader remain the same code/operations
  (`object_io.rs:114-129`, `:85`, `:98`). The Local table clone shares native state.
- New nonempty remote objects get one streamed range GET, with before/after
  length checks, checked byte totals and no range chunking or collected payload
  (`remote/objects.rs:362-407`). Empty/inherited payloads receive no identity GET.
  New list/manifest bytes still have their necessary native decode plus separate
  identity pass; captured list/manifest hashes are reused. No throughput/RSS
  improvement is claimed from static review.
- URI checks are cheap metadata work. Rechecking strict scope in the admitted
  read helper keeps that helper safe without another persistent state type.
- Candidate registration can already have succeeded when final local binding
  refuses a returned location. As before, the caller must retain and reconcile
  the exact identifier/metadata URI; this path neither retries nor cleans up.
- Host scratch, admission, release reads and retention still have local seams.
  `register_output` also still selects Local. The absence of a remote Engine
  constructor prevents this intermediate integration from advertising those
  unqualified operations.

## Test behavior and remaining scope

The populated tests use maintained native Avro writers and snapshot builders,
then exercise actual configured HTTP readers. They assert exact object closure,
snapshot/schema/row count, original-owner reuse, one new bulk identity GET,
zero inherited identity GETs, endpoint binding, and pre-read scope refusals
(`tests/pinning.rs:118-313`). The synthetic payload deliberately does not claim
Parquet decoding or remote dataset qualification.

The extended HTTP fixture adds per-URI contents/request recording while retaining
existing size/short-stream behavior (`remote/objects/tests.rs:252-373`). Existing
verify tests still traverse the extracted core; candidate write/cancellation tests
still traverse the shared create helper. Local sharing, schema, compaction,
backfill and incremental suites exercise the migrated Engine callers. Those
existing assertions are summarized in the first review; their paths remain
unchanged apart from the new pin helper.

Parent reports the focused remote-object suite passed on the applied code,
including the new populated fixtures, after correcting the native Table imports.
I did not independently execute or inspect that run. The complete repository
gate was still running when this review was written. A stalled remote pin-read
test is not present; worker ownership is supported by static tracing and the
existing primitive/candidate cancellation tests. Full remote Engine, provider
maintenance, restore and performance acceptance remain separate gates.

## Conclusion

**VERDICT: APPROVE** for this storage boundary and local Engine pin migration,
subject to the normal repository gate. The change resolves the prior findings,
keeps the shared algorithm, preserves captured identities and local behavior,
and has concrete production callers without a new public abstraction.

Coverage of changed scoped behavior: adequate. Confidence: high for static
traces; runtime qualification is limited to the parent's reported focused run.
