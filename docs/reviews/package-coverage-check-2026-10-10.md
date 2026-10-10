# Package coverage check

**The direct-package inventory is complete. Keep the pinned family and finish the
existing ranked improvements.** This independent source-only check found no
omitted package that changes the
[utilization audit](dependency-utilization-audit-2026-10-10.md) or its
[follow-up](dependency-audit-followup-2026-10-10.md). Useful utilization means
delegating required work efficiently, not enabling every optional feature.

Scope: committed `cb0a0498b81b11da68bb2a533823a685f9e461fd` plus the selected-key
patch explicitly identified below. This check parsed current manifests and
tracked Python imports, reused unchanged package research, and inspected the
provider seam against installed DataFusion 53.1.0 and official tagged source.
Only this report was written. No source edits, builds, tests, benchmarks,
dependency resolution, corpus reads, asset downloads or cleanup were performed.

## Coverage matrix

Every current direct Rust dependency is named below. Exact versions, features,
call sites and individual decisions remain in the
[existing matrix](dependency-utilization-audit-2026-10-10.md#complete-direct-rust-coverage).

| Packages/boundary | Coverage and disposition |
| --- | --- |
| `arrow`, `parquet` | Shared arrays, native encoders/decoders, IPC and writer sizing covered. Finish early column selection; qualify native row-group layout separately. |
| `datafusion` | Runtime, statistics, relational operators and source adapters covered. Keep optimizer correctness guards; additional provider distinction below. |
| `iceberg`, `iceberg-catalog-sql`, `sqlx` | Table/schema/manifest I/O, writers, conditional commits, catalog pooling and restore copying covered. Reuse admitted metadata. |
| `tokio`, `futures`, `async-trait` | Owned admission, blocking work, cooperative execution, stream composition and upstream dynamic interfaces covered. Preserve bounded reader activation. |
| `axum` | Framing, extraction and demand-driven bodies covered. Keep chunk-owned memory admission. |
| `serde`, `serde_json`, `blake3` | Typed metadata, bounded serialization and streamed hashing covered. Existing hash-only JSON cleanup remains small. |
| `fs2`, `uuid`, `mimalloc` | Locks/free-space APIs, immutable random names and the CLI allocator covered. No additional feature justified. |
| `anyhow`, `clap`, `tracing`, `tracing-subscriber` | CLI errors, arguments and structured diagnostics covered. No replacement framework needed. |
| `tempfile`, `tower` — development | Fixture isolation and service testing covered. |
| Rust build boundary | No direct build dependencies or project `build.rs`. Cargo test grouping is committed; the earlier “in progress” wording is superseded by its [review](cargo-test-grouping-review-2026-10-10.md). |
| Python adapter/build: `dagster`, `pydantic`, `setuptools` | Manifest/import coverage confirmed. Keep Rust lifecycle ownership and the thin maintained adapter. |
| Python experiments: `duckdb`, `pyarrow`, `pyiceberg`, `psutil`, `blake3`, `numpy` | Fresh syntax-tree inspection found no additional external imports. Preserve independent comparison. Freeze the actual environment before another controlled experiment; existing receipts are not a complete install lock. |
| Material transitives/re-exports | Existing coverage includes Arrow/DataFusion component crates, HTTP/Bytes, storage/cache, SQLx/SQLite, codecs, allocator bindings, parsing and build macros. Additional `datafusion-catalog`/`datafusion-physical-plan` streaming inspection below. |

This is complete direct-package coverage plus selected material transitive
interfaces, **not** a source audit of every `Cargo.lock` implementation. The
tracked-manifest search found no additional package manifest or install lock.
Python inspection parsed syntax without importing or executing modules.

## Additional API boundary

**Keep the native selected-column provider.** DataFusion 53.1.0's `StreamingTable`
can reduce boilerplate for an already shaped stream, but `PartitionStream`
receives only execution context. Its execution plan opens the stream and then
calls `RecordBatch::project`; it does not pass requested columns into the reader
or supply native row-count statistics. Replacing our provider wholesale would
discard useful existing behavior. This was confirmed in installed source and the
official [catalog implementation](https://raw.githubusercontent.com/apache/datafusion/53.1.0/datafusion/catalog/src/streaming.rs)
and [execution implementation](https://raw.githubusercontent.com/apache/datafusion/53.1.0/datafusion/physical-plan/src/streaming.rs).

The [native provider](../../src/query.rs#L157) composes indices before opening
Iceberg and supplies selected-pin row counts. For fixed-schema composition, a
single sequential `PartitionStream` over those existing providers can still use
the maintained streaming table. Giving each child its own partition does not
establish an active-reader bound. This is an integration constraint, not a newly
observed failure or a requirement for another abstraction.

Minimum checks if adopted: confirm selected columns reach the native reader;
compare values/addresses with the full path; check active readers as chain length
grows; exercise later read failure, early drop and permit lifetime. Reuse existing
checks. This documentation itself needs no new payload scan or benchmark.

## Priorities and current state

No new finding supersedes the existing order:

1. **Finish selected-key adoption.** The working-tree
   [changed-key path](../../src/engine/incremental.rs#L367) resolves each side's
   indices, calls `read_projected_file` and shares array handles. Root reports
   formatting and strict Clippy pass; runtime qualification is pending because
   the shared volume cannot currently accommodate linking. No I/O or throughput
   improvement has been measured.
2. **Reuse captured metadata/inventories**, preserving admission lifetime, before
   introducing another cache. The known deep metadata clone and hash-only JSON
   buffers remain small related cleanups.
3. **Skip the large-side cardinality scan when the constrained side is unique**,
   retaining the matched-duplicate fallback and qualifying both branches.
4. **Qualify native row-group sizing with memory and disk together**, including
   old/new coexistence, spill, failed attempts, peak additional disk and retained
   bytes. Smaller files alone do not prove lower complete-path cost.

Keep guarded selection, occurrence interpretation, retention decisions and
resource ownership as thin domain adapters. Underlying packages cannot infer
those promises. No new package upgrade, optional feature or performance claim
follows from this check.

## Inventory identity

The lockfile and Dagster manifest identities match the utilization audit:

- `Cargo.lock`: `1a42088d740c961f0e4261c46bf60e2efebaa5cda5d61caecd8c5de2a5cc906f`
- `adapters/dagster/pyproject.toml`: `45bdcdb17448c397e0309847814702aa3d9ca035c0b4eb1bb1c4d8ee2138c478`
- Current `Cargo.toml`: `f3a0798a2ae7163a57521be70cfed0fe6cab47dea7bff744369e86affd1e02a2`

These are SHA-256 identities. Cargo target organization changed; dependency
declarations did not. Earlier retained experiment versions are historical
evidence, not a newly checked installed Python environment.
