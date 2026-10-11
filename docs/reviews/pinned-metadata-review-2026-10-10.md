# Pinned metadata: independent review

**Verdict: approve the corrected admission split.** Focused checks and the
complete library suite pass; the full integration gate was still running when
this review was finalized. The first full gate exposed a real tiny-budget regression that this
review's original approval missed: the general blocking helper charged an
unneeded 1 MiB copy buffer. The root separated already-admitted work from copy
workers while retaining the same job limiter and cancellation ownership. The
revised path hashes and decodes the same bytes, removes the prior second payload
read and preserves native table interpretation. Admission covers opening
metadata; it does not establish a bound on all returned tables' lifetime heap.

This blind static review covers `src/storage/pinned.rs`, the `src/storage.rs`
replacement/re-export, the direct `flate2` declaration and production caller
migration on 2026-10-10. I applied the repository, Rust and semi-formal review
guidance and traced installed Iceberg 0.10.1, the existing shared budget, relevant
callers and fixture assertions. No author note or unrelated change-chain review
was read. I ran no builds, tests, formatting, provider calls or code edits; runtime
evidence below comes from the retained root-owned logs.

## Findings and limits

### F1 — Resolved in source: fixed copy-buffer reservation broke small readers

The [first full gate](join-metadata-full-check-2026-10-10.log) records 11 query
fixture failures with `capacity` at the newly budgeted pin open. Their reader
budgets were below the general blocking helper's fixed 1 MiB memory reservation,
even though the small metadata's actual stored/decoded reservations fit. The
new opener does not use that copy buffer. This was an introduced compatibility
regression, not evidence that those tests needed larger budgets.

The repair at `src/budget.rs:130-168` keeps `blocking` as the existing copy/hash
wrapper, owning its 1 MiB permit inside the closure. New `blocking_admitted`
acquires the same nonwaiting `blocking_jobs` permit and moves it and the supplied
closure into `spawn_blocking`. `src/storage/pinned.rs:46-74` uses this new entry
point; its closure already owns serialized bytes and acquires decode admission
before parsing. A failed job admission drops the closure and its reservation.
After spawning, cancellation drops only the awaiting join handle: the queued or
running worker still owns its job permit and all captured memory permits until
completion. There is no intervening wait while permits are acquired and no new
independent budget or worker pool.

The source fix resolves the incorrect 1 MiB floor. The retained
[focused checks](pinned-metadata-small-budget-tests-2026-10-10.log) confirm that
the query fixtures, budget tests and pinned metadata tests pass without
increasing those budgets.
The added `canceled_small_worker_keeps_its_owned_bytes_and_worker_slot` test
(`src/budget.rs:393-431`) directly covers the new path with a 128 KiB budget.
It reserves the full application-admission half, moves that permit into a
channel-stalled worker, waits for the worker to start, and cancels its caller.
It then independently requires replacement-byte admission and another worker
to refuse with `capacity`. After releasing the stalled worker, it waits for the
job permit and requires both admissions to recover. That wait also makes the
final assertions independent of worker-exit scheduling. The test is meaningful
and covers the previously missing memory assertion. The retained
[second full gate](join-metadata-full-check-2026-10-10-2.log) records this test
passing and the complete library suite passing; I did not run these checks. No duplicate metadata test is needed solely for the fixed floor because
the existing query fixtures caught it.

No further implementation changes requested. Preserve these explicit scope
limits when reporting the result:

- **Temporary admission is not retained-table admission.**
  `src/storage/pinned.rs:48,71-74` releases serialized/decode permits when the
  worker exits. The parsed metadata then escapes in `Table` (`:83-90`). Some
  existing callers keep their own admitted captures; others, such as
  `src/engine/read.rs:151-160` and the provider constructed at
  `src/query.rs:45-69`, do not attach a dedicated parsed-metadata permit to every
  table. This was not introduced as a new retained-memory promise, and the public
  documentation correctly assigns lifetime accounting to callers (`pinned.rs:23-25`).
  Do not report this patch as closing whole-process or all-retained-metadata
  bounds.
- **One payload read still has metadata requests.** The code performs two size
  checks around one bounded range read (`pinned.rs:51-59`). On remote storage
  those can be separate requests. They detect observed length changes, while
  the digest binds the bytes actually parsed. They do not atomically bind an
  object generation; immutable ownership remains required, as documented
  (`:27-28`). No remote latency or request saving is established here.

## Function and caller trace

| Function / seam | Reference | Inputs → outputs | Verified behavior |
| --- | --- | --- | --- |
| `open_pin` | `src/storage/pinned.rs:14-16` | Pin + caller budget → native table | Uses maintained filesystem FileIO and the same shared implementation. |
| `open_pin_with_io` | `src/storage/pinned.rs:34-90` | Pin + supplied FileIO + shared budget → readonly table | Bounds claimed stored size before I/O; reserves shared bytes; reads once inside owned blocking work; checks size/digest; decodes; checks exact snapshot/schema; preserves FileIO, location, identifier and runtime. |
| `decode` | `src/storage/pinned.rs:93-115` | Bound bytes + decoded limit → native `TableMetadata` | Plain JSON uses native Serde type. Gzip uses maintained `GzDecoder`, a limit-plus-one reader, Serde end-of-input checking and rejection of compressed trailing bytes. |
| `capacity` / `corrupt` | `src/storage/pinned.rs:118-126` | Failure condition → structured refusal | Retains explicit metadata-capacity and changed-object identity errors; snapshot/schema mismatches still use `pin_mismatch`. |
| Module and dependency wiring | `src/storage.rs:7-9`; `Cargo.toml:44`; `Cargo.lock` DocSpec dependency entry | Public old/new APIs + maintained codec | Old double-read implementation is removed. `flate2` was already resolved transitively; the lockfile adds it to DocSpec without replacing a codec implementation or upgrading its resolved version. |
| `Budget::blocking` / `blocking_admitted` | `src/budget.rs:130-168` | Copy/hash closure or already-admitted closure → owned worker result | Shared nonwaiting worker admission and `spawn_blocking`; the old wrapper adds its copy-buffer reservation, while metadata supplies its own byte reservations. The worker owns permits until exit even if its awaiting caller disappears. |
| Ordinary and combined reads | `src/engine/read.rs:151`; `src/engine/backfill.rs:197-229` | Existing caller budget → opened native sources/providers | Propagates the already shared budget; it does not create a fresh default budget inside production opens or add data-file verification. |
| Full/incremental processing and source interpretation | `src/engine/write.rs:43-64,135-143`; `src/engine/incremental.rs:61-65,200-206`; `src/engine/refresh.rs:534-556` | Admitted input pins → native tables/captures | Existing budget flows through source interpretation, related input registration and captured-layout checks; logical and schema checks remain. |
| History/schema/verification | `src/engine/backfill.rs:1105-1119`; `src/engine/backfill/verification.rs:454-505`; `src/engine/schema.rs:588-599`; `src/engine.rs:2311-2323` | Existing verification/history budget → native metadata | Helper signatures now carry their caller's budget. Existing deep checks remain deep; normal metadata opens do not acquire that payload work. |

## Native meaning and ownership

The compatibility hypothesis was that replacing `StaticTable` could lose native
compression handling, metadata conversion or the selected snapshot/schema.
The inspected upstream implementation confirms preservation:

- Iceberg's `TableMetadata::read_from`
  (`iceberg-0.10.1/src/spec/table_metadata.rs:450-479`) detects gzip by magic bytes
  and deserializes `TableMetadata` using Serde. The new path uses the same native
  deserializer. Its bounded streaming gzip decoder avoids the upstream whole
  decompressed byte vector. No project-owned native schema parser was introduced.
- Upstream `StaticTable::from_metadata_file`
  (`iceberg-0.10.1/src/table.rs:364-385`) builds the table with metadata, location,
  identifier, FileIO, runtime and readonly mode. The new builder supplies the
  same fields. The old explicit current snapshot/schema comparisons remain.
- Native filesystem FileIO performs synchronous file metadata, open, seek and
  `read_exact` inside its async methods
  (`iceberg-0.10.1/src/io/storage/local_fs.rs:100-138,238-264`). Moving those calls,
  BLAKE3 hashing and JSON/gzip parsing into `Budget::blocking_admitted` avoids doing them
  on the async executor thread. The supplied FileIO remains attached to the
  returned table, so remote native reads need no filesystem-only fallback.

The serialized-byte reservation is moved into the owned closure before any read.
The decoded reservation is acquired inside that same worker before parsing.
Errors drop both; cancellation of the awaiting task does not drop the worker's
captures. `Handle::block_on` runs the native I/O future inside that admitted
blocking worker, not on a runtime worker. No nested budget worker call was found
in the migrated production paths inspected here.

The decoded JSON byte cap is deliberately distinct from parsed-object heap use.
Gzip's limit-plus-one sentinel rejects oversized decoded JSON, including trailing
whitespace; exactly-limit valid JSON can still reach EOF. Requiring a complete
single gzip member and rejecting trailing compressed bytes is a documented
tightening. The previous native decoder could ignore trailing material; the
new behavior does not silently accept a partial compressed object.

## Test behavior and qualification

The new module's inspected assertions cover:

- Plain and gzip metadata through injected in-memory FileIO, exact native
  metadata/location/identifier and retained I/O (`pinned.rs:189`).
- Stored and decoded caps, exact-cap acceptance, and oversized claims rejected
  even when the object path does not exist (`:215`).
- Digest/size changes, growth and snapshot/schema mismatch (`:248`).
- Truncated/corrupt gzip trailers, extra bytes/members, excessive decoded
  whitespace and invalid trailing JSON (`:293`).
- The local public entry point (`:322`).

The existing `canceled_blocking_work_keeps_admission_without_stalling_the_executor`
test (`src/budget.rs:360` after the helper split) explicitly covers worker
admission surviving caller abort on a single-thread executor through `blocking`.
The repaired opener delegates directly to the same owned-worker mechanism via
`blocking_admitted`. The added small-worker cancellation test (`:393-431`)
checks both that entry point's worker slot and its caller-owned memory, including
recovery after actual worker exit. There is no new counted/stalled FileIO test
proving one-read behavior or cancellation specifically during this metadata
read; the single-read and metadata moved-permit conclusions remain static code
traces. The retained focused log records the query, budget and pinned suites
passing. The second full-gate log records the added cancellation test and the
complete library suite passing; integration execution was still underway when
this report was finalized. This review does not claim a completed full gate.
No source or data reprocessing is required to populate a report.

**Conclusion:** APPROVE the repaired source. Focused checks and the complete
library suite pass; the full integration gate remains pending at this checkpoint. The implementation removes one metadata payload pass per open and adds
stored/decoded byte caps without imposing the unrelated copy-worker buffer floor.
It shares the existing worker limiter and retains cancellation ownership. These
are structural improvements, not measured throughput, remote qualification or a
whole-process memory guarantee. The failed gate is retained and supersedes the
original unqualified compatibility assessment. Static ownership tracing and
retained targeted/library results support the repair; final integration results
must be recorded separately when that gate finishes.

## Root validation after review

The [completed repository gate](join-metadata-full-check-2026-10-10-2.log) exits
successfully: formatting, strict all-target/all-feature lint, library,
integration, restore, worker-initialization and documentation checks pass. The
existing external-data physical-plan probe remains explicitly ignored. The
earlier failed gate remains retained. A documentation-only note also makes
Tokio runtime ownership explicit for asynchronous FileIO; provider cancellation
and shutdown remain part of future remote qualification.
