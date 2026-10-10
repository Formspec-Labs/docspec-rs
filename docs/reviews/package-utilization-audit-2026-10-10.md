# Package utilization: current implementation check

**Keep the package stack. The remaining opportunities are to use its existing
readers and relational operations more efficiently, not enable every feature.**
Read selected columns, reuse admitted metadata, skip redundant join-validation
scans, and measure native row-group layout before changing it. The newly added
sequential comparison adapter uses maintained streaming components appropriately.

This audit covers HEAD `75b40495cda4f0ca3caedbd1aff5e7cf895e1e3f` and the current
forward-comparison working changes. It reuses the
[complete utilization matrix](dependency-utilization-audit-2026-10-10.md),
[resolved feature inventory](../dependency-efficiency-audit-2026-10-10.md#appendix-complete-resolved-direct-dependency-feature-sets)
and [coverage check](package-coverage-check-2026-10-10.md), then checks current
callers and the relevant exact-version crate source. Official upstream source
was consulted for the changed streaming seam and remaining reader/writer APIs.
The Rust skill's performance and dependency guidance applies.

Only this report was written. No builds, tests, benchmarks, dependency resolution,
payload scans, downloads of corpus assets, cleanup or source changes were performed.
The independent forward-comparison correctness review remains separate. This
report supplies no new elapsed-time, memory or storage measurement.

## What changed since the existing audit

- **Selected-key reading is implemented.**
  [Changed-file discovery](../../src/engine/incremental.rs#L367) resolves each
  captured schema's key indices and calls `read_projected_file` at line 440.
  It shares the returned Arrow column handles rather than selecting from a
  full-width decoded batch. The empty-key and equivalent-representation branches
  remain. This closes that audit's direct call-site omission; measured savings
  still belong to the implementation owner's qualification.
- **Sequential composition now uses the maintained streaming table.**
  [SequencePartition](../../src/query/sequence.rs#L68) supplies one partition to
  `StreamingTable`, with `then` and `try_flatten` opening the next child only
  after the previous stream ends. The installed `futures-util` 0.3.34 source
  clears the exhausted child before polling its outer stream. Child DataFrames
  retain their own native column selection. Metadata admission remains owned by
  the provider and its streams. This is an appropriate small adapter, not a
  second execution engine.
- **Native target grouping is committed.** [Cargo.toml](../../Cargo.toml#L7)
  groups ordinary integration suites and keeps process-sensitive suites separate.
  Retain this standard Cargo solution to repeated linking; do not treat build
  artifacts as dataset storage amplification.

`StreamingTable` is suitable for already shaped child streams. Its pinned
`PartitionStream` API receives execution context, not requested columns; its
execution applies batch projection after opening the partition. Keep the native
provider for early column selection and physical statistics. Replacing it
wholesale would lose those capabilities. The upstream table defaults to finite
input. [Pinned catalog implementation](https://raw.githubusercontent.com/apache/datafusion/53.1.0/datafusion/catalog/src/streaming.rs),
[pinned execution implementation](https://raw.githubusercontent.com/apache/datafusion/53.1.0/datafusion/physical-plan/src/streaming.rs).

## Outstanding work, ranked by useful impact

These are known prior findings reconfirmed from current source, not newly discovered
omissions. None requires a new package.
Their order is a practical starting point; measurements determine whether a
larger change pays for itself.

| Priority and status | Concrete maintained integration | User value, tradeoff and minimum check |
| --- | --- | --- |
| **Next: reuse metadata within an immutable operation.** | [Located execution](../../src/query.rs#L324) opens [ordinary occurrences](../../src/storage/occurrences.rs#L71), which calls [snapshot_files](../../src/storage.rs#L519). That traverses retained snapshots and unique manifests; capture then plans files. Reuse the existing admitted `Arc<OccurrenceScan>` and `Table` when their exact capture is already available. | With K executions over H snapshots/M unique manifests, avoid repeated O(K(H+M)) inventory work through one admitted capture. This is medium integration work, not a timing result. Preserve exact pin, schema/default interpretation, file membership, occurrence identity and admission lifetime through the last reader. Measure planning calls, requests and bytes separately from payload I/O. No general cache is needed. |
| **Next: remove the common redundant cardinality scan.** | [join_plan](../../src/engine/refresh.rs#L532) runs [matched-key duplicate probes](../../src/engine/refresh.rs#L642) before the main join. Use DataFusion grouping to establish whether the constrained side is unique first; if it is, the other-side probe is unnecessary. Preserve the current matched-key proof if duplicates exist. | Medium implementation: can remove one population scan per constrained-side proof when that side is unique. The duplicate branch adds a constrained-side pass. Qualify unmatched duplicates, both null modes, composite keys, join kinds and self-join roles; inspect scan counts and measure both branches. Do not create a key-index service. |
| **Then: qualify native row-group sizing.** | [Native writer](../../src/storage.rs#L264) still caps row-group rows at processing `batch_rows`. Parquet exposes separate row and encoded-byte thresholds; the flat writer already uses maintained memory/flush APIs. | Larger groups may reduce footer overhead and retained output, while increasing writer and selected-page reader memory. Compare complete write/read costs, requested bytes, peak memory, peak additional disk and retained bytes. Include old/new coexistence, scratch and failed attempts. A smaller output alone does not establish a better lifecycle. |
| **Small cleanup: share admitted metadata and hash directly.** | [Origin table construction](../../src/engine/backfill/origins/checks.rs#L249) deep-clones metadata even though `TableBuilder::metadata` accepts `Arc<TableMetadata>`. [Request fingerprint](../../src/engine.rs#L2061) and [physical schema identity](../../src/storage/inspection.rs#L50) still allocate JSON solely to hash it; use existing `storage::json_digest` on the unchanged values. | Removes one metadata clone and temporary JSON byte vectors. Preserve ownership/admission and exact identity bytes, including schema normalization. This removes no payload scan or disk write. |
| **Later: select scalar backfill inputs.** | [File preparation](../../src/engine/backfill/preparation.rs#L609) uses the full reader. Existing DataFusion expressions identify dependencies, but [expression compilation/evaluation](../../src/engine/backfill/expressions.rs#L128) binds indices and occurrence columns against the complete schema. | Read only expression dependencies plus origins for narrow computations on wide data. This needs rebinding against the selected schema, not a one-line reader replacement. Preserve rename/default/null/constant-only behavior, metadata-sensitive functions and retained reuse. |
| **For the changes above: retain existing scan metrics.** | [Reader construction](../../src/storage.rs#L131) consumes `ScanResult::stream()` and discards its metrics handle. Clone the native `ScanMetrics` handle in the qualification path. | Establish requested-byte effects without a second payload scan or custom decoder. Its byte counter increments requested ranges before I/O; it does not measure physical disk traffic, successful delivery or manifest planning. No new routine reporting workflow is justified. |

The supported metadata-sharing setter, writer thresholds and scan counters are
confirmed in pinned upstream source:
[Iceberg table builder](https://raw.githubusercontent.com/apache/iceberg-rust/v0.10.1/crates/iceberg/src/table.rs),
[Parquet properties](https://raw.githubusercontent.com/apache/arrow-rs/58.4.0/parquet/src/file/properties.rs),
[Iceberg scan metrics](https://raw.githubusercontent.com/apache/iceberg-rust/v0.10.1/crates/iceberg/src/arrow/scan_metrics.rs).

[open_pin](../../src/storage.rs#L714) also hashes metadata and then opens it
again through `StaticTable::from_metadata_file`. Prefer a previously admitted
immutable handle. A later single-read adapter must preserve size/digest checks,
selected snapshot/schema checks and maintained compressed-metadata decoding.
The current explicit manifest loads bypass the native metadata cache; increasing
that cache does not remove them. Its pinned Moka weigher counts owning structs,
not their complete heap contents, so cache capacity is not a process-memory bound.

## Complete package coverage

The exact versions, requested/resolved features, upstream references and individual
call-site decisions in the [complete direct-package matrix](dependency-utilization-audit-2026-10-10.md#complete-direct-rust-coverage)
and [feature appendix](../dependency-efficiency-audit-2026-10-10.md#appendix-complete-resolved-direct-dependency-feature-sets)
remain applicable: fresh manifest/lockfile hashes match. This check did not rerun
Cargo resolution. The compact disposition below names every direct package;
unchanged details are reused rather than investigated again.

| Packages | Active use and current decision |
| --- | --- |
| `arrow`, `parquet` | Shared batches, compute, IPC, native codecs/footers/writers. Selected-key adoption is implemented; native row-group layout and scalar input selection remain above. Avoid blanket string-view/dictionary changes or indexes without eligible reader/benefit evidence. |
| `datafusion` | Native statistics/selection, shared runtime, joins, aggregates and windows. New sequence adapter uses maintained streaming correctly. Reconfirm redundant cardinality probe above; keep optimizer correctness guards. |
| `iceberg`, `iceberg-catalog-sql`, `sqlx` | Native data/schema/manifest I/O, conditional catalog SQL and consistent restore copy. Reuse shared admitted metadata. No replacement catalog, DocSpec SQL store or new backup driver justified. |
| `tokio`, `futures`, `async-trait` | Owned permits, blocking work, stream composition and upstream dynamic interfaces. Preserve lazy activation and admission lifetime; worker threads, partitions and active readers are distinct limits. |
| `axum` | Typed HTTP routing/extraction and demand-driven bodies. Keep existing request bounds and chunk-owned admission; no producer queue or blind retry middleware. |
| `serde`, `serde_json`, `blake3` | Typed retained metadata, bounded serialization and streamed hashing. Finish two pure hash-only JSON sites; keep persistence/size-admission buffers and copy/hash boundaries. |
| `fs2`, `uuid`, `mimalloc` | OS locks/free-space, immutable names, CLI allocator. Keep; moving only locks to std would leave the space API. Extra UUID/allocator modes offer no demonstrated value. |
| `anyhow`, `clap`, `tracing`, `tracing-subscriber` | CLI errors, arguments and bounded structured diagnostics. Keep maintained interfaces and library refusal codes; no new framework or per-row events. |
| `tempfile`, `tower` — dev direct | Fixture isolation and router service tests. Keep; durable candidates have different ownership, and transitive retry features do not authorize automatic mutation retries. |
| Rust build | No direct build dependencies or project `build.rs`. Current native Cargo target grouping addresses repeated linking. Feature trimming requires an actual resolved-graph reduction while preserving SQL functions/codecs. |
| `dagster`, `pydantic`, `setuptools` | Adapter/build constraints remain `==1.13.16`, `>=2,<3`, `>=68`. Maintained resource/op/job, field bounds and standard packaging; Rust owns lifecycle decisions. Extra assets/partitions/concurrency policies need a concrete workload. |
| `duckdb`, `pyarrow`, `pyiceberg`, `psutil`, Python `blake3`, `numpy` | Fresh syntax-tree inspection of tracked Python confirms no additional external imports. Keep independent batched qualification, resource sampling and vectorized preparation. Retained version receipts are historical, not a complete environment lock; freeze the actual environment before another controlled experiment. |

This is complete **direct-package** coverage plus selected material transitives,
not a source review of every `Cargo.lock` implementation. The previous transitive
matrix remains applicable. Fresh version checks and relevant boundaries:

| Material transitive family | Efficient use and limit |
| --- | --- |
| Arrow 58.4.0 and DataFusion 53.1.0 component crates | Already supply buffers, compute, native bridges, operators and planning. `datafusion-catalog` / `datafusion-physical-plan` now supply the finite sequential adapter. Keep early native selection rather than post-decode-only projection. |
| `object_store` 0.13.2 and Iceberg `FileIO` | Maintained filesystem/object and ranged I/O. Reader controls already expose prefetch/coalescing/concurrency; measure actual remote requests/overfetch before tuning. `opendal` is absent from this lockfile. |
| `moka` 0.12.16 | Native metadata cache. Explicit inventory loads bypass it and its upstream weigher omits complete heap contents; reuse admitted captures before enlarging it. |
| `sqlx-core` / `sqlx-sqlite` 0.8.6, `libsqlite3-sys` 0.30.1 | Native pooling, guarded catalog writes and SQLite `VACUUM INTO`; no measured catalog-copy pressure justifies another driver. |
| `futures-util` 0.3.34, `tokio-util` 0.7.19 | Maintained stream/runtime glue. Exact flatten source drops the previous child before advancing; no custom scheduler is needed. |
| `bytes` 1.12.1, `hyper` 1.12.0 and HTTP/body crates | Maintained HTTP framing and `Bytes::from_owner` in [IPC](../../src/http/ipc.rs#L73) preserve outgoing chunk admission. |
| `zstd` 0.13.3 / 0.14.0, `snap` 1.1.2, `lz4_flex` 0.13.1, `brotli` 8.0.4 and other codecs | Preserve supported inputs and selected Zstandard output. Duplicate versions alone do not establish a useful optimization; require compatible graph reduction and measurement. |
| `libmimalloc-sys` 0.1.49, parser/proc-macro/build support | Allocator bindings, authored SQL parsing and generated interfaces remain upstream work; no custom replacement recommended. |

One active file still permits multiple ranges. Iceberg's defaults are 512 KiB
footer prefetch, 1 MiB coalescing gap and up to 10 merged range requests; they trade
requests, overfetch and memory. [Pinned reader configuration](https://raw.githubusercontent.com/apache/iceberg-rust/v0.10.1/crates/iceberg/src/arrow/reader/mod.rs).

Keep field-ID/default interpretation, occurrence identity, guarded lifecycle
selection, retention decisions, cancellation-safe admission and independent
qualification as thin domain adapters. Packages cannot infer those promises.
The compatible pinned family and current correctness guards remain appropriate;
no new cache, scheduler, database, writer, decoder or blanket feature expansion
is justified. [Cargo features are additive](https://doc.rust-lang.org/cargo/reference/features.html#feature-unification),
so root-default cleanup alone may remove no compiled capability.

## Inventory identity

Fresh SHA-256 identities match the prior coverage check:

- `Cargo.lock`: `1a42088d740c961f0e4261c46bf60e2efebaa5cda5d61caecd8c5de2a5cc906f`
- `Cargo.toml`: `f3a0798a2ae7163a57521be70cfed0fe6cab47dea7bff744369e86affd1e02a2`
- `adapters/dagster/pyproject.toml`: `45bdcdb17448c397e0309847814702aa3d9ca035c0b4eb1bb1c4d8ee2138c478`

Reusing the unchanged inventory avoids repeating package research. The actionable
follow-through is to implement and qualify the ranked omissions above, retaining
the smallest change that improves the complete path.
