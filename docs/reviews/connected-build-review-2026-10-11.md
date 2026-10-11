# Connected initial Build review

Verdict: **APPROVE**, subject to parent-owned compilation and tests. Reviewed the applied/formatted changes after `596ac9a`, originating from patch `7e220aec123fde5554b5fde1ee5c1be6dae498241a674080ed9191cbd846ae06`. Static review only: no builds, provider calls, or source edits by this reviewer.

## Findings and dispositions

- **Closed, correctness:** saved candidate validation regenerated `MetadataLocation`, whose constructor generates a fresh UUID. `src/engine/write/build.rs:83–103` now parses and checks the retained canonical initial URI instead. Retries preserve the original identifier, metadata bytes and URI.
- **Closed, durability:** direct creation exposed a partial final metadata file after worker loss. `src/storage/tables.rs:237–266` now writes and syncs an unreferenced sibling, publishes with no-clobber hard link, and syncs the final directory. Existing-file reconciliation verifies exact bytes and syncs that directory before acknowledgement (`src/storage/object_io.rs:117–122`). A crash may retain a temporary; it cannot publish its partial bytes as the candidate.
- **Closed, test coverage:** the first fixture bypassed actual Engine retry. `src/engine/write/build/tests.rs:143–161` now resumes through `run_until("processing")` and asserts a per-Engine counter remains unchanged. The counter increments before Parquet registration/query planning (`build.rs:201–209`). Original producer removal is exercised; retained input availability remains required evidence.
- **Closed, local behavior:** the branch rewrite initially hid `update_source`. The tuple in `src/engine.rs:944–1005` preserves both the captured source and its memory reservation through completion.
- **Closed, performance:** lifecycle commit initially nested its existing guard inside the new append guard, repeating a catalog load. `src/catalog.rs:405` now passes `self.catalog.as_ref()` to append while preserving its earlier preflight. The shared append guard checks the exact base once at this boundary, avoiding the extra request without changing selection conditions.

## Function and state trace

| Path | Verified behavior |
|---|---|
| `prepare_source`, `src/engine.rs:944` → `process_initial_build`, `src/engine/write/build.rs:107` | Initial Build with no base uses the shared path. Other local operation shapes preserve their existing implementation; unsupported remote shapes still refuse. |
| `existing_candidate` / `check_candidate_intent`, `build.rs:215,236` | Resolve operation-generation intent before opening retained Parquet. Validate request/revision, exact input, namespace, owner, configured location and paired protection. |
| `reserve_candidate`, `build.rs:265` | Produces one bounded metadata serialization, UUID and URI; atomically commits exact intent plus prefix protection before object creation or registration. Lost lifecycle response reconciles that same key. |
| `Objects::create_candidate_metadata`, `src/storage/object_io.rs:90` | Local atomic publication; remote existing-object check only on recovery, then conditional creation. Conflict/uncertainty reconciles the exact URI; read errors never mean absence. The worker retains its byte reservation after caller cancellation. |
| Registration/recovery, `build.rs:136–190` | An existing registration is loaded first. Registration error checks only the retained identifier, preserves absence as failure, and validates any recovered table. A single committed append supplies counts and is pinned before query entry. |
| `check_candidate`, `build.rs:379` | Requires original UUID/location/schema/properties/layout and a single parentless Append snapshot, or exact empty initial metadata/URI. Selected gzip bytes remain valid: codec is not inferred from future-write properties. |
| `storage::append`, `src/storage.rs:449` → `GuardedCatalog`, `src/catalog.rs:575–595` | Refuses changed metadata or URI; native refresh cannot adopt newer application state and append again. Expected snapshot requirements remain enforced at the native catalog update. |

The explicit FileIO binding survives native transaction refresh: pinned Iceberg `transaction/mod.rs:218–245` substitutes the refreshed Table only when metadata or URI differs; the guard rejects both differences. Returned candidates are rebound before pinning. Native writer paths derive from the protected initial location and fixed fresh properties.

I traced the other production append callers in lifecycle commits, updates, incremental key/history tables, compaction and backfill. They append to a captured lifecycle state or fresh candidate, and none requires implicit adoption of a newer native head. The new shared guard preserves that intent. Empty file lists still return the existing table unchanged.

## Resources, ownership and evidence

Intent decoding is admitted before loading; preparation uses a bounded serializer and an owned blocking reservation; lifecycle record/history limits still bound retained metadata. Parsed initial metadata is reused rather than parsed again. This adds bounded metadata state, not a payload copy. Reservations are conservative and overlap while the intent is committed; this review establishes bounded/refusing behavior, not a measured RSS ceiling or throughput gain.

`candidate-attempt/` is recognized by inventory and operation-recovery retirement (`inventory.rs:1514`, `recovery.rs:27,241`, `retirement.rs:84`). It does not enter `owners::visit_creation`; an intent or readable URI cannot grant ownership. Native output becomes qualified creation evidence only through normal processing completion. Remote destructive maintenance and public remote startup remain outside this slice.

## Tests and practical limits

The recovery fixture at `build/tests.rs:11` exercises lost intent-commit response, protection before effects, partial unreferenced temporary, actual native append before phase completion, exact reused pin/metrics, no repeat Parquet/query entry through Engine, and duplicate/null readback. The stale-append fixture at `:187` checks refusal without new manifest/metadata files and one selected snapshot. Parent fixed its returned reader to an owned pinned stream before shutdown.

Registration-response loss and a simultaneous competing-append race are statically traced here; these new tests do not inject either case. Their actual remote-provider qualification remains outstanding. The fixture does not simulate OS power failure. An unfinished empty result without a native snapshot may recompute until its processing phase is durable; completed phases still reuse their saved result. Competing unfinished writers can produce unselected files, but the guarded update prevents double append. Native commit recovery may redo pin identity work when no durable phase retained those identities; it does not repeat completed SQL.

This is a connected initial native-processing slice, not complete hosted lifecycle qualification. Parent-owned focused and repository gates must establish the applied source's local compatibility before commit.

Final follow-up: parent reports both focused candidate tests passed and the library suite is running. This reviewer confirmed the final direct-catalog append call in source; execution and final gate receipts remain parent-owned. No actionable finding remains.

## Runtime follow-up: memory lifetime regression

The parent-owned library run in `docs/reviews/connected-build-library-2026-10-11.log` exposed failures not established by the initial focused tests: existing low-memory Engine paths exhausted shared admission, and one legacy test selected `candidate-attempt/.../processing` as if it were a completed `phase/` record. The earlier static approval is conditional on correcting and rerunning those failures; do not describe this original candidate source as locally compatible.

The production cause is overlapping maximum-sized reservations: `process_initial_build` holds `2 * max_metadata_bytes` for its entire query/pin lifetime, while `reserve_candidate` reserves the same maximum again for preparation and lifecycle commit. The retained objects are usually much smaller. The smallest correction is an owned permit travelling with retained candidate/native metadata: charge saved records by existing `reserve_decoded` before allocation; use one transient worker-owned serialization reservation for new metadata, then shrink it to actual retained size before commit. Release fields and permit after their last identity check, or retain only that small actual-size charge. Do not raise test limits. The separately decoded operation/request/protection records must be covered; the candidate key alone does not account for them.

Inventory already has an explicit graph/decoded-row allowance. Recovery validation must preserve its own decode admission for the candidate plus cross-record bindings; nested native JSON decoding must not escape merely because `CandidateAttempt` is a small Rust struct. These are conservative admission estimates, not an RSS proof.

## Corrected runtime findings

Reviewed the parent's applied correction in `src/engine/write/build.rs`. `RetainedCandidate` now owns the saved per-key admission through use. New preparation moves one transient reservation into `blocking_admitted`, then shrinks it to a conservative estimate of retained strings/native metadata before lifecycle commit. Concurrent-winner adoption drops the losing candidate and reservation before decoding the retained winner. The fixed maximum-sized outer reservation is gone. `validate_record` admits separately decoded operation/request/protection records for recovery callers; inventory continues using its existing decoded-row allowance. No resource ceiling was raised.

The broader suite then exposed a second real regression: an explicitly retired processing phase could reuse its old native candidate or reject a newly admitted input. The corrected candidate key at `build.rs:39–48` binds the operation revision, prior processing-row revision, and digest of the exact admitted `ObjectRef`. Lookup uses the current processing generation; fresh commit and lost-response adoption recheck it. Historical record validation requires the stored processing revision to precede the intent, without requiring equality to the current processing row. Thus history remains inspectable while explicit phase retirement produces a new candidate. Operation-recovery key parsing recognizes the expanded identity. Existing retirement assertions remain unchanged.

These corrections close the static causes of the reported failures. Final approval remains conditional on the parent's unchanged low-memory and phase-retirement regression runs; prior focused-test success alone does not establish that result. This review did not run tests or alter implementation.

The subsequent full gate exposed a separate scope-parser regression introduced while extending the candidate key: existing `prepared-changes/{operation}` and `protection/{id}` branches had been dropped. Reviewed the parent's restoration against the base diff. Both existing branches are restored verbatim; the final diff only adds the candidate/release key families and their bounded, identity-checked collection loops. Their pending records do not replace the original protection records. The unchanged retirement suite must confirm this correction; the earlier static review did not establish it.

Follow-up: the parent reports the unchanged retirement regression suite passes, including the previously failing cases. This reviewer reconfirmed that the current `recovery.rs` diff preserves both original scope branches. Final repository-gate status remains parent-owned.
