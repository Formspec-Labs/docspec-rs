# Package audit: current delta

**Keep the stack. The main missing evidence is remote-package efficiency and
actual I/O savings.** The implementation follow-up below closes the identified
redundant join scan and metadata reread; the native-read follow-up also supplies
requested-range evidence. More package inventories will add less value than
collecting the missing measurements during useful work.

This bounded static check compares the five user-attached package reports in
`/tmp/codex-remote-attachments/01a116b5-a9a6-7363-acec-cc4bcb984f9a/78eed266-e922-415d-b761-a6d3b57061aa/`
with [the later followthrough](dependency-utilization-followthrough-2026-10-10.md),
commit `42ab0c3`, and the current working tree. No builds, tests, network calls,
credentials, corpus reads, or implementation changes were involved. This checks
changes and unresolved decisions, not every transitive implementation.

| Priority | What is actually missing | Smallest useful next step |
| --- | --- | --- |
| **1. New remote dependency coverage** | The old manifest identities and “OpenDAL is absent” statement predate [the optional remote feature](../../Cargo.toml#L33). `iceberg-catalog-rest` 0.10.1, `iceberg-storage-opendal` 0.10.1, and direct `reqwest-rest` 0.12.28 now exist. The [foundation review](r2-catalog-foundation-review-2026-10-10.md) covers server commit guards, synthetic isolation, TLS, timeouts and redaction; it does not qualify bulk transfer efficiency. | Extend the current ledger with these packages and the resolved remote feature graph. During the first real transfer workload, record S3/REST requests, retries, transferred bytes, buffering/peak memory and interrupted-upload reuse. Keep default/local and remote build footprints separate. No second client or transfer implementation is justified. |
| **2. Evidence for delivered projection improvements** | Native and scalar column selection exist, but [ordinary scans](../../src/storage.rs#L136) and [occurrence scans](../../src/storage/occurrences.rs#L234) still consume `.stream()` without retaining `ScanMetrics`. The reports already identify this; it remains unimplemented measurement support, not an unaudited package. | Preserve the native metric handle for the next targeted qualification. Compare actual requested data ranges and physical scan executions, plus elapsed time and resource peaks. Count metadata requests separately. Do not add a second payload pass or mandatory report to ordinary reads. |
| **3. Avoid the redundant large-side cardinality scan** | [Join probes](../../src/engine/refresh.rs#L678) still semi-join against the other relation before grouping constrained keys. The recommended duplicate-first proof has not landed. Metadata capture reuse does not eliminate this payload scan. | Prove the constrained side unique using existing DataFusion grouping; use the current matched-key proof only when duplicates exist. Preserve legal unmatched duplicates, composite/null-key behavior and both constrained sides. Measure unique and duplicate branches before claiming a speedup. |
| **4. Close the existing storage experiments when they matter** | [Native row-group size](../../src/storage.rs#L270) still follows compute batch rows; [DataFusion configuration](../../src/budget.rs#L110) does not select spill compression; [open_pin](../../src/storage.rs#L717) still verifies metadata and reopens it through `StaticTable`. These are covered but unimplemented/unmeasured, not newly discovered omissions. | First identify the actual dominant cost. Test native layout with writer **and reader** memory/disk; test compression only on real spilling work; reuse admitted metadata before building a cache. Keep peak extra disk, retained bytes, retries/failed outputs and old/new coexistence visible. Avoid changing defaults merely because an API exists. |

## What the new remote stack already does

The adapter uses maintained REST and S3 implementations, restricts the optional
storage feature to S3, and injects the configured catalog at startup
([remote adapter](../../src/storage/remote.rs#L34)). The initial REST TLS omission
and missing request deadlines were found and repaired in the independent review;
they are not outstanding findings from this delta check.

Pinned source shows that `iceberg-storage-opendal` already applies
`TimeoutLayer` inside `RetryLayer` (`src/lib.rs:358-368`), delegates streaming
writers to `op.writer` (`src/lib.rs:526-530`), and uses maintained S3 multipart
machinery (`opendal-service-s3-0.57.0/src/backend.rs:1097-1111`). OpenDAL's default
HTTP client clones a shared client
(`opendal-core-0.57.0/src/raw/http_util/client.rs:50,92-96`), so constructing
native operators does not by itself establish a missing connection pool.
The next audit boundary is the actual chosen chunk/concurrency/retry behavior
and its resource cost during a representative transfer. A tiny lifecycle-table
probe cannot establish that result.

The two Reqwest versions serve different maintained dependencies. The REST
client uses 0.12; OpenDAL uses 0.13. Removing the explicit REST TLS feature because
the S3 stack already supports TLS would restore the reviewed defect. Their
presence, or the presence of multiple crypto providers, is not evidence of
duplicate data processing. Do not force version unification without a compatible
upstream change and measured reason.

## Stale open items and retained limits

Several attached reports describe older code. These items should not be reopened:

- Scalar dependency reads now use `read_scalar_file`
  ([preparation](../../src/engine/backfill/preparation.rs#L615)); the committed
  qualification and conservative full-width fallback are recorded in
  [STATUS](../STATUS.md#scalar-reads-select-consumed-columns).
- Changed-key discovery uses `read_projected_file`
  ([incremental](../../src/engine/incremental.rs#L472)).
- The two pure JSON hashing sites use `json_digest`
  ([fingerprint](../../src/engine.rs#L2086),
  [schema identity](../../src/storage/inspection.rs#L50)); origin checks pass
  the shared metadata handle ([checks](../../src/engine/backfill/origins/checks.rs#L249)).
- Ordinary integration suites are already grouped, with the process-sensitive
  targets kept separate ([Cargo targets](../../Cargo.toml#L10)).

The later followthrough also closed the earlier lack of an explicit Avro
disposition: native writers use the uncompressed codec despite codec features
being available. That is already documented; no custom manifest writer is needed.

Python environment reproducibility remains an acknowledged limit: the adapter
manifest has bounded Pydantic and build-tool ranges, and the retained experiment
versions are not a complete installation lock. Capture the actual environment
before the next controlled experiment; do not rerun accepted data merely to fill
an old environment field. The package reports explicitly avoid claiming that
static coverage proves full-lifecycle throughput, complete resource bounds, or
hosted support. This delta finds no reason to change that conclusion.

## Root follow-up after the static review

The [resolved host feature record](r2-resolved-features-2026-10-10.json) now
records the optional REST/OpenDAL/Reqwest stack against exact manifest/lockfile
digests. Existing resolved package versions were preserved; the optional graph
adds the maintained S3 and REST dependencies. The
[final adapter checks](r2-catalog-final-check-2026-10-10.log) pass, and the
[live provider probe](r2-catalog-provider-2026-10-10.jsonl) passes actual server
conflict and fresh-client lost-response reconciliation in a synthetic namespace.
These close the new feature-inventory and tiny provider-seam gaps. They do not
measure representative transfer throughput, request amplification, buffering,
multipart recovery or complete remote publication. The local measurement and
implementation priorities above remain unchanged.

## Implementation follow-up

The preceding table records the review snapshot. Its duplicate-first cardinality
and metadata-reread implementation items are now addressed:

- Join checks first group the constrained input using DataFusion. Unique keys
  skip the partner's payload scan; duplicates retain the existing matched-key
  proof. The [targeted fixture](duplicate-first-cardinality-tests-2026-10-10.log)
  observes zero partner executions and one constrained execution for the unique
  many-to-one branch, versus one and two respectively for duplicate branches.
  Exact full-join/self-join cases retain null rules, unmatched duplicates and
  occurrence evidence. The [independent review](duplicate-first-cardinality-review-2026-10-10.md)
  approves. These execution counts are not representative throughput, bytes or
  peak-memory measurements; duplicates intentionally add constrained-side work.
- [Pinned metadata](../../src/storage/pinned.rs) now passes one bounded FileIO
  range read to hashing and native deserialization, replacing hash-then-reopen.
  Pre/post size checks remain. Both local and injected I/O use the same code;
  gzip uses maintained `flate2` and bounded Serde parsing without a decoded-byte
  copy. `flate2` 1.1.10 was already transitive; its direct declaration adds no new
  locked package/version. Caller budgets now govern opening work. The
  [focused checks](pinned-metadata-tests-2026-10-10.log) cover local/memory I/O,
  configured stored/decoded limits, gzip integrity and pin identity. This does
  not yet qualify real-provider pinned release reads or the lifetime of all
  returned metadata/cache allocations.

At that checkpoint, native scan metrics, representative remote requests/transfers/retries, update
and history scaling, and measured native layout/spill choices remain open.
Complete remote publication and maintenance remain in [STATUS](../STATUS.md);
an injected metadata reader does not complete that delivery.

Both changes pass the [final repository gate](join-metadata-full-check-2026-10-10-2.log).
The [metadata review](pinned-metadata-review-2026-10-10.md) records an initial
fixed-buffer admission regression caught by the earlier gate and the corrected
small-budget/cancellation behavior. The correction shares the existing worker
limiter and introduces no new executor or cache.

## Native-read and immutable-write follow-up

Native scan observation is now implemented behind the existing `test-util`
feature. It retains maintained Iceberg `ScanMetrics` with constant additional
state across files; ordinary readers allocate no observer. The
[focused fixture](native-scan-observation-error-tests-2026-10-10.log) observes
5,313,491 requested bytes for full reads and 1,050,136 for narrow reads while
checking exact values, schemas and independent occurrence locations. Scalar
per-file selection follows the same path. This closes the missing metric handle
and bounded fixture evidence, not representative elapsed time, backend retries,
network traffic, manifest planning or aggregate memory. The
[blind review](native-scan-observation-review-2026-10-10.md) approves the scope.

Footer inspection now passes two disjoint native FileIO ranges into Parquet's
maintained push decoder instead of reopening a local file and rereading its tail.
The [tests](shared-footer-tests-2026-10-10.log) prove range boundaries, short-read
refusal and local/memory parity. A direct **dev-only** `bytes` declaration uses
the existing locked 1.12.1 package for the counting `FileRead` fixture; no new
package/version or production dependency was introduced. The earlier manifest
digests remain historical records, not claims about the latest manifest.

For remote immutable writes, [Cloudflare documents conditional PUT support](https://developers.cloudflare.com/r2/api/s3/api/).
The current table does not separately specify conditional multipart completion.
The [actual-provider probe](r2-conditional-create-provider-2026-10-10.jsonl)
therefore tests both: competing conditional PUT and delayed competing multipart
completion preserve exact winning bytes. This establishes the needed provider
primitive through boto3 1.42.33; it does not qualify the Rust uploader or claim
transport-response-loss recovery. The bounded synthetic objects and losing
unfinished multipart upload remain recorded and retained.

Pinned dependency inspection narrows the next implementation choice:

- OpenDAL 0.57.0 exposes `writer_with(...).if_not_exists(true)`. Its S3 core sends
  `If-None-Match: *` for both PUT and multipart completion
  (`opendal-service-s3-0.57.0/src/core.rs:327`, `:1069`). Reuse this maintained
  capability and qualify the actual Rust path; do not build an S3 signer.
- Iceberg FileIO 0.10.1 does not expose this option: `OutputFile::write` overwrites,
  and `writer` forwards an unconditional writer (`src/io/file_io.rs:363-376`).
  Its OpenDAL adapter's operator construction is private. Remote composition
  therefore still needs an explicit maintained conditional-writer connection;
  an `exists` check followed by FileIO write cannot provide it.
- The already pinned `object_store` 0.13.2 supports conditional single PUT, but
  `PutMultipartOptions` lacks a create-only mode (`src/lib.rs:1813-1830`), and
  its S3 multipart completion uses `CompleteMultipartMode::Overwrite`
  (`src/aws/mod.rs:434-446`). Switching upload stacks solely to obtain safe
  multipart creation would not solve this boundary at the pinned version.

Do not add a second durable selection, alternate transfer lifecycle, custom
signer or broad storage framework to bridge this API boundary. Remote lifetime,
actual buffering, interrupted transfer reuse and request/byte measurements still
need the complete Rust path. Neither this fixture nor static package support
completes [STATUS](../STATUS.md).

The combined implementation passes the
[full repository gate](native-readers-full-check-2026-10-10.log), and the
[shared-footer/provider-probe review](shared-footer-provider-probe-review-2026-10-10.md)
approves its bounded scope. The exact provider-probe source digest matches the
retained run. No accepted corpus workload was repeated.
