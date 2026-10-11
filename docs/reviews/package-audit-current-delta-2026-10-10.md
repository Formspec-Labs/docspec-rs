# Package audit: current delta

**Keep the stack. The new omission is remote-package efficiency coverage; the
largest remaining local opportunity is an already identified redundant join
scan.** More package inventories will add less value than closing those concrete
items and collecting the missing measurements during useful work.

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
