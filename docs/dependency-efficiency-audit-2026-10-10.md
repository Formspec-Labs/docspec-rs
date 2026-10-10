# Package efficiency audit

**Keep the current dependency family. The remaining useful work is to avoid redundant cardinality scans, extend selected-column reading to scalar preparation, and qualify native row-group sizing. Reuse immutable metadata where a repeated open remains.** The [current follow-up](#current-follow-up-2026-10-10) rechecks every direct package, enabled defaults, relevant duplicate dependencies and maintained adapter alternatives. It distinguishes completed improvements from work still in progress; its status and priorities supersede earlier snapshots below. Enabling every optional feature would increase the surface to maintain without establishing a benefit.

The latest [independent completeness review](reviews/package-efficiency-completeness-review-2026-10-10.md)
confirms every direct Rust and Python package against the unchanged manifests and
current call sites. It adds one bounded experiment: compare DataFusion's existing
spill compression with the uncompressed default when a representative query
actually spills. Both codecs are already enabled. Measure peak scratch, memory,
time and exact results before changing defaults; this review establishes no new
speedup or disk-saving result.

The [earlier utilization check](reviews/dependency-utilization-audit-2026-10-10.md)
rechecks package coverage at `2a5ae51`. It identifies changed-key discovery as a
remaining caller that can use the existing selected-column reader, and records
the reader-concurrency constraint for composing change history. Its ranked
actions are retained as research history; the detailed upstream analysis remains
useful. Cargo target grouping changes build organization, not dependency features.

The [independent coverage addendum](reviews/package-coverage-check-2026-10-10.md)
checks the current manifests/imports and the maintained streaming-table boundary.
It finds no omitted direct package or new reason for feature expansion. Follow
[current status](STATUS.md#dependency-efficiency-audit) for selected-key adoption
and its qualification; earlier audit snapshots describe the code when inspected.

## Current follow-up: 2026-10-10

This follow-up now covers `a3d695fa9b4644d046f202a90de9c7ad9d9678a3` and the
inspected working compaction-recovery retirement changes. The renewed check
parsed every direct Rust declaration, Cargo.lock, the Dagster manifest and Python
imports; confirmed unchanged manifest digests and complete package dispositions;
rechecked the remaining opportunity call sites; and compared the maintenance
adapter with exact installed Iceberg source and version-pinned official sources.
It reused the earlier offline, locked Cargo metadata/tree evidence because no
dependency declaration or lockfile changed. The audit ran no builds, tests,
benchmarks, corpus reads or asset downloads and changed only this report.
Coverage means every **direct** production/development/build package and the
material transitive facilities listed here. It does not mean every transitive
implementation was reviewed. Prior exact-version package research and the
feature appendix remain applicable; no feature set changed.

Fresh SHA-256 identities are `f3a0798a2ae7163a57521be70cfed0fe6cab47dea7bff744369e86affd1e02a2`
for Cargo.toml, `1a42088d740c961f0e4261c46bf60e2efebaa5cda5d61caecd8c5de2a5cc906f`
for Cargo.lock and `45bdcdb17448c397e0309847814702aa3d9ca035c0b4eb1bb1c4d8ee2138c478`
for the Dagster manifest. The original manifest identity farther below predates
Cargo target grouping. `cargo metadata --offline --locked --all-features --format-version 1`
confirms the direct resolved-feature appendix. Host production reachability was
checked separately with `cargo tree --offline --locked --target aarch64-apple-darwin --edges normal,build`;
lockfile presence alone does not establish
that a crate is compiled or linked for this target.

### Completed integration and active work

- **Implemented:** early native column selection and selected-key discovery,
  BLAKE3's reader, streamed JSON fingerprints, shared original-table metadata,
  and constrained full-join inventory capture. In particular,
  [register_join_inputs](../src/engine/refresh.rs#L524) captures each input once
  for the cardinality probes and main query, sharing exact self-join inputs.
  [Original-table construction](../src/engine/backfill/origins/checks.rs#L241)
  now passes the admitted metadata `Arc` directly. These are no longer pending
  recommendations. [STATUS](STATUS.md#dependency-efficiency-audit) names their
  retained author checks; this audit did not rerun them or measure their speed.
- **Implemented:** [sequence](../src/query/sequence.rs#L27) uses DataFusion's
  finite `StreamingTable` and Futures `then`/`try_flatten`. The pinned flatten
  implementation drops the completed child before opening the next one. Keep
  selected-column providers underneath this already-shaped stream composition;
  no custom scheduler, concatenation engine or fan-out queue is needed.
- **Implemented at `a3d695f`:** the root-owned
  [open_captured_source](../src/engine/write.rs#L43) helper is shared by source
  replacement and [incremental discovery](../src/engine/incremental.rs#L64).
  It checks logical identity, row count and public schema before reading an
  admitted compacted representation. This consolidates interpretation checks;
  it is not a general metadata cache or elimination of every metadata open.
  The implementation owner retained passing
  [focused checks](reviews/compacted-related-targeted-2-2026-10-10.log) and a
  [repository gate](reviews/compacted-related-full-check-2026-10-10.log), with an
  [independent review](reviews/compacted-related-review-2026-10-10.md). These are
  implementation evidence, not execution by this audit or a speed measurement.
  Do not duplicate this change from an earlier audit's recommendation.
- **Implemented at `8566235`:** [compaction recovery retirement](../src/engine/retention/compaction_recovery.rs)
  separates recoverability of an old rewrite from the selected replacement's
  lifetime. The [protection helper](../src/engine/compaction.rs#L16) and
  [inventory handling](../src/engine/retention/inventory.rs#L1230) preserve the
  replacement and logical metadata without automatically retaining every old
  payload through that selection. Other promises remain independent roots.
  The [retained repository gate](reviews/compaction-retirement-full-check-2026-10-10.log)
  includes real obsolete-layout removal fixtures. This addresses retained disk
  amplification through existing lifecycle rules; the package audit itself
  establishes neither full-scale reclamation nor measured disk savings.

### Complete current direct-package matrix

Every Rust entry below was matched to Cargo.lock and current callers. The
[detailed package matrix](#every-direct-rust-dependency) retains pinned official
API links and feature rationale; the current disposition here supersedes its
older call-site status.

| Direct package / resolved version | Current maintained use and disposition |
| --- | --- |
| `anyhow` 1.0.104 | [CLI result boundary](../src/main.rs#L304), `bail!` and `ensure!`. Keep stable library refusal codes; no additional error framework or feature is justified. |
| `arrow` 58.4.0 | [Selected arrays](../src/storage/selection.rs), shared batch columns, compute and [IPC](../src/http/ipc.rs). Keep zero-copy handles; scalar input selection remains below. Array-handle cloning is not a payload copy, while slices can retain their backing allocation. |
| `async-trait` 0.1.92 | [Native catalog](../src/catalog.rs#L567) and [TableProvider](../src/query.rs#L147) implement upstream dynamic async interfaces. Keep the required adapter; a local syntax replacement cannot change those interfaces. |
| `axum` 0.8.9 | [Router](../src/http.rs#L32), bounded JSON extraction and streamed Arrow bodies. Keep native framing/backpressure. Unused direct defaults are identified below; no new transport is needed. |
| `blake3` 1.8.7 | [Reader](../src/storage.rs#L418), [Serde writer](../src/storage.rs#L56), admission copy/hash and [export sink](../src/engine/export.rs#L806). Maintained replacements are adopted. Keep required write-and-hash boundaries; defer Rayon/mmap without evidence. |
| `clap` 4.6.7 | [Derived CLI](../src/main.rs#L29). Native parsing/help already replace custom parsing. No pipeline efficiency change is justified. |
| `datafusion` 53.1.0 | [Shared runtime](../src/budget.rs#L85), [physical statistics](../src/query.rs#L284), relational execution and [stream composition](../src/query/sequence.rs#L27). Keep optimizer correctness guards and admitted comparison ranking. Duplicate-first cardinality proof is the remaining relational opportunity. |
| `fs2` 0.4.3 | [OS-held lock](../src/budget.rs#L191) and [free-space check](../src/engine/export.rs#L476). Standard-library locking alone would not remove the free-space dependency; no replacement is justified. |
| `futures` 0.3.34 | [Demand-driven streams](../src/storage.rs#L138) and [sequential flattening](../src/query/sequence.rs#L77). Keep bounded activation; upstream consumers still enable default executor utilities. |
| `iceberg` 0.10.1 | [Selected native scans](../src/storage/schema.rs#L96), snapshots/manifests, [native writers](../src/storage.rs#L273), transactions and references. Keep the decoder/writer; use its metrics and shared metadata rather than another cache or decoder. |
| `iceberg-catalog-sql` 0.10.1 | [SqlCatalogBuilder](../src/storage.rs#L75), native pool and conditional catalog commits. Keep; no measured contention calls for another catalog or tuning the pool. |
| `mimalloc` 0.1.52 | [CLI global allocator](../src/main.rs#L26). Keep existing allocator; library consumers choose their allocator. No new flags or upgrade justified by this audit. |
| `parquet` 58.4.0 | [Native properties](../src/storage.rs#L264), maintained encoders/footers, [flat writer](../src/engine/export.rs#L447) and memory-based flush. Native row-group sizing remains below; no blanket codec/index change. |
| `serde` 1.0.229 | [Typed durable/public data](../src/model.rs#L8) and [lifecycle records](../src/catalog.rs#L547). Derives provide maintained serialization. Keep exact types and ordering; no new serialization format. |
| `serde_json` 1.0.151 | [Direct hash serialization](../src/storage.rs#L60), bounded metadata parsing/persistence and CLI/HTTP JSON. Pure hash buffers are removed; buffers used for persistence or admission still serve a purpose. |
| `sqlx` 0.8.6 | [Native SQLite restore copy](../src/engine/restore.rs#L807) uses `VACUUM INTO`, then integrity checks; SQL catalog uses its `Any` driver. Keep native backup/database facilities. MySQL/Postgres are not active host production dependencies. |
| `tokio` 1.53.2 | [Owned semaphore/blocking admission](../src/budget.rs#L131), [worker lifetime](../src/engine/worker.rs), network and cooperative yields. Keep permits inside work that survives caller cancellation. CLI workers, DataFusion partitions and native file/range concurrency are distinct controls. |
| `tracing` 0.1.44 | [Structured capacity/lock diagnostics](../src/budget.rs#L206) and lifecycle events. Keep bounded fields and phase-level diagnostics; no per-row logging or replacement backend. |
| `tracing-subscriber` 0.3.23 | [Environment-filtered stderr setup](../src/main.rs#L305). Keep maintained filtering; no measured need to narrow its behavior or enable another output format. |
| `uuid` 1.27.0 | [Native file IDs](../src/storage.rs#L281), operation/object names and parsing. Keep random immutable naming; v7 is not a demonstrated storage optimization here. |
| `tempfile` 3.27.0 — dev | [Isolated test workspace](../tests/worker_initialization.rs#L4), maintained cleanup. Keep; durable completed phases must retain their different lifetime. |
| `tower` 0.5.3 — dev direct | [HTTP ServiceExt/oneshot tests](../tests/http_read.rs#L18). Keep maintained service harness. Runtime transitive retry/timeout features do not mean DocSpec installs mutation-retry middleware. |

The Python import check parsed adapter, script and experiment files without
executing modules. It found the same external packages below; local helpers and
the standard library add no third-party package requirement. Qualification
versions are retained receipt observations, not a newly verified installation.

| Python package / declaration or retained version | Current maintained use and disposition |
| --- | --- |
| `dagster ==1.13.16` | [Resource/op/job and explicit retries](../adapters/dagster/src/docspec_dagster/definitions.py#L9). Keep the thin scheduler and unchanged native request on uncertain outcomes. [Native operation APIs](https://dagster.io/docs/api/dagster/ops) already supply the scheduling primitive. |
| `pydantic >=2,<3` | [Field bounds](../adapters/dagster/src/docspec_dagster/definitions.py#L13). Keep native validation; the standalone HTTP client still needs exact-byte/protocol checks. [Field documentation](https://pydantic.dev/docs/validation/latest/concepts/fields/). |
| `setuptools >=68` — build | [Manifest](../adapters/dagster/pyproject.toml#L1) uses standard discovery/build backend. Keep; no runtime bulk-data work. [Build configuration](https://setuptools.pypa.io/en/latest/userguide/pyproject_config.html). |
| `duckdb` 1.4.4 — qualification | [Oracle controls](../experiments/comments-e2e/oracle.py#L118), direct Parquet views and exact duplicate-aware equality. Keep accepted four-thread settings and spill bounds. Later/current documentation does not qualify different settings in this pinned environment. |
| `pyarrow` 23.0.0 — qualification | [Batch preparation](../experiments/comments-input/prepare.py#L59), footer/schema inspection and typed expectations. Keep maintained batching; the [23.x batch-reader API](https://arrow.apache.org/docs/23.0/python/generated/pyarrow.parquet.ParquetFile.html) supports projection, but full source preservation requires all selected source fields. |
| `pyiceberg` 0.12.0 — qualification | [Static snapshot/file planning](../experiments/comments-e2e/oracle.py#L161). Keep independent membership/schema interpretation and explicit unsupported-delete refusals. [Native API](https://py.iceberg.apache.org/api/) is guidance, not Rust/Python feature parity. |
| `psutil` 7.0.0 — qualification | [Process-tree sampling](../experiments/comments-input/run.py#L83). Keep measurement separate from allocator/engine allowances; sampled peaks are lower bounds. [Native sampling APIs](https://psutil.readthedocs.io/stable/) avoid another monitor. |
| Python `blake3` 1.0.11 — qualification | [Integrity pass](../experiments/comments-e2e/integrity.py#L70) combines digests and before/after identity checks. Keep one read; separate convenience whole-file calls would repeat it. |
| `numpy` — preparation, version not retained | [Batch row mask](../experiments/comments-input/prepare.py#L60) uses vectorized integer operations. Keep; no Python row loop to replace. Record its actual environment if preparation is rerun. |

No additional direct build package or project `build.rs` was found. Python
qualification still lacks a complete reproducible installation lock; freeze the
actual environment before the next controlled run instead of assuming an old
executable path reconstructs it.

### Remaining work, ranked by concrete work avoided

| Priority | Current evidence and maintained option | Smallest validation before acceptance |
| --- | --- | --- |
| **First: skip the common redundant cardinality scan.** | [join_queries](../src/engine/refresh.rs#L585) still semi-joins constrained-side keys against the other input before grouping duplicates. Prove constrained-side uniqueness with maintained [DataFusion grouping](https://raw.githubusercontent.com/apache/datafusion/53.1.0/datafusion/core/src/dataframe/mod.rs) first; no duplicates means the large-side proof scan is unnecessary. Keep the matched-key proof when duplicates exist. This removes an O(N) probe scan in the unique branch; the duplicate branch adds constrained-side work. Inventory capture already removes repeated metadata discovery, not these payload scans. | Exact results/refusals for unmatched and matched duplicates, composite keys, both null modes and both constrained sides; inspect actual scan execution counts for unique and duplicate branches. Measure both branches before claiming a speedup. |
| **Next: select scalar input dependencies.** | [File preparation](../src/engine/backfill/preparation.rs#L609) still reads the complete file before [evaluating expressions](../src/engine/backfill/expressions.rs#L128). Reuse `read_projected_file` for required value/origin columns and rebind DataFusion physical expressions to that selected schema. Work can scale with selected width instead of full row width; rows still need evaluation. | Cover renamed/reordered fields, defaults, nulls, constant-only expressions, metadata-sensitive functions and stored origins. A damaged-unused-column fixture plus native requested-byte counters can establish excluded decoding without a full benchmark. |
| **Then: qualify native row-group sizing.** | [Native writer](../src/storage.rs#L268) still sets row-group rows equal to processing batch rows. [Parquet's pinned properties](https://raw.githubusercontent.com/apache/arrow-rs/58.4.0/parquet/src/file/properties.rs) support separate row/encoded-byte bounds. Larger groups can reduce footer overhead and retained bytes, but increase writer and selected-page reader memory. | First test large row groups with small returned batches under admission. Then compare write/read costs, footer/output bytes, RSS, scratch, peak additional disk and retained bytes. Include old/new coexistence and failed attempts; the flat-export result is not native-writer proof. |
| **Bounded follow-up: reuse remaining repeated metadata opens.** | [open_pin](../src/storage.rs#L714) still hashes metadata, then `StaticTable` rereads it. Prefer an already admitted immutable table/capture at repeated call sites. For a later single-read implementation, use the native [table metadata setter](https://raw.githubusercontent.com/apache/iceberg-rust/v0.10.1/crates/iceberg/src/table.rs) with exact identity and selected schema/snapshot checks; preserve compressed-metadata decoding. Do not redo the completed join capture or metadata-Arc fix. | Count opens/planning requests as history/file count grows; verify corruption, gzip metadata, exact pin binding, final-owner permit release and failures. Keep independent verification boundaries. A process-wide cache is not needed. |
| **Measurement support for these changes:** preserve the reader's existing metrics. | [Ordinary reads](../src/storage.rs#L131) and [occurrence reads](../src/storage/occurrences.rs#L232) consume `.stream()` and lose access to [Iceberg ScanMetrics](https://raw.githubusercontent.com/apache/iceberg-rust/v0.10.1/crates/iceberg/src/arrow/scan_metrics.rs). Clone its handle before consuming the stream in qualification/requested diagnostics. | Label it requested data/delete-file range bytes, including attempted reads; it excludes manifest planning and is not physical disk I/O. Reuse existing inventories for retained disk; do not rescan payloads merely to report savings. |

The [shared FairSpillPool](../src/budget.rs#L88) only bounds participating
operators. The pinned [hash join](https://raw.githubusercontent.com/apache/datafusion/53.1.0/datafusion/physical-plan/src/joins/hash_join/exec.rs)
has no spill cure for an oversized build. Keep accurate native statistics and
the existing [comparison admission](../src/engine/comparison.rs); no new custom
join/window engine is justified. [Tokio's blocking documentation](https://docs.rs/tokio/1.53.2/tokio/task/fn.spawn_blocking.html)
confirms why worker-owned permits must survive caller cancellation. Increasing
`target_partitions` does not split the single native source partition at
[query.rs:276](../src/query.rs#L276); increasing file/range concurrency needs a
measured bottleneck and concurrent-buffer accounting first.

### Maintained alternatives and the precise adapter gaps

`iceberg-datafusion` **0.10.1 is version-compatible**: its published manifest
accepts DataFusion 53.1, Iceberg 0.10 and Parquet 58. The inspected package's
`.cargo_vcs_info.json` identifies upstream commit
`04ae06bdb15a6fd7c7927d29d4e0f6a33de0f1f9`.
The [official dependency family](https://raw.githubusercontent.com/apache/iceberg-rust/04ae06bdb15a6fd7c7927d29d4e0f6a33de0f1f9/Cargo.toml)
and [static provider](https://raw.githubusercontent.com/apache/iceberg-rust/04ae06bdb15a6fd7c7927d29d4e0f6a33de0f1f9/crates/integrations/datafusion/src/table/mod.rs)
support explicit snapshot reads. Snapshot pinning is **not** the reason to reject
that integration. The catalog-backed variant intentionally reloads current heads;
that variant would not preserve selected immutable inputs.

The actual gaps in its [scan implementation](https://raw.githubusercontent.com/apache/iceberg-rust/04ae06bdb15a6fd7c7927d29d4e0f6a33de0f1f9/crates/integrations/datafusion/src/physical_plan/scan.rs)
are no injection point for our admitted captured reader, no physical row-count
statistics override, no stored-origin/physical-position output, and one source
partition. It calls native `to_arrow` internally. Our selected-schema `_file`
workaround, complete-file checks and admission lifetimes would still need an
adapter. Its internal limit filters later batches rather than terminating that
stream; an outer limit can stop consumption, so this is a caution about that
primitive, not proof every limited SQL query scans everything. Keep the current
native provider until an alternative qualifies these exact behaviors with less
code. Upstream append/commit integration also does not replace DocSpec's
multi-table guarded selection and recoverable phase ownership.

The [maintained StreamingTable](https://raw.githubusercontent.com/apache/datafusion/53.1.0/datafusion/catalog/src/streaming.rs)
is already used where appropriate. Its [execution](https://raw.githubusercontent.com/apache/datafusion/53.1.0/datafusion/physical-plan/src/streaming.rs)
projects emitted batches, so it cannot alone replace early native column
selection. Likewise, DataFusion's Parquet/object-store cache does not govern the
Iceberg decoder. Explicit manifest inventory loads at
[snapshot_files](../src/storage.rs#L519) bypass Iceberg's cache; its
[native Moka weigher](https://raw.githubusercontent.com/apache/iceberg-rust/v0.10.1/crates/iceberg/src/io/object_cache.rs)
does not include complete heap-owned entries. Enlarging cache weight is not a
memory-bound proof or a replacement for admitted request-local reuse.

The native [Parquet writer](https://raw.githubusercontent.com/apache/iceberg-rust/v0.10.1/crates/iceberg/src/writer/file_writer/parquet_writer.rs)
already owns schema conversion, encoding, compression, statistics and rolling
files. Its completed `DataFileBuilder` does not return a BLAKE3 content digest;
`current_written_size` reports written/encoded buffering, not total writer memory.
Our native-output digest pass therefore remains a documented exception until its
cost justifies a storage-writer interception. The flat writer's small
[HashingFile](../src/engine/export.rs#L836) instead hashes bytes actually accepted
by the file and syncs completion: a plain BLAKE3 reader would require another pass.
The [HTTP IPC adapter](../src/http/ipc.rs#L73) uses maintained Arrow StreamWriter,
Axum bodies and `Bytes::from_owner`; its custom piece retains the permit with the
chunk and bounds serialization. Neither a producer queue nor another stream
format removes that ownership requirement.

The working compaction-retirement path still needs application-owned lifetime
rules. Pinned [Iceberg transactions](https://raw.githubusercontent.com/apache/iceberg-rust/v0.10.1/crates/iceberg/src/transaction/mod.rs)
expose native append and snapshot-expiration actions; they do not own DocSpec's
selected representation, recovery records or cross-dataset object grants.
[ExpireSnapshotsAction](https://raw.githubusercontent.com/apache/iceberg-rust/v0.10.1/crates/iceberg/src/transaction/expire_snapshots.rs)
updates metadata and leaves physical file removal to higher-level maintenance.
It also combines explicitly named snapshots with age-based expiry by default;
calling it is not equivalent to removing only a reviewed set of DocSpec objects.
Do not substitute it for the current protection inventory and exact deletion
reconciliation, or claim freed disk from expiration alone.

The [compaction writer](../src/engine/compaction.rs#L410) already delegates
encoding, compression and rolling files to the maintained native writer; its
batch adapter shares Arrow arrays. [Equivalence checking](../src/engine/compaction.rs#L580)
compares sequential batch slices across differing chunk boundaries and retains
completion for reuse. That small adapter expresses required value/order/origin
semantics; replacing it with a full relational sort or another decoder would add
work. Existing selected file sizes supply rewrite/input/output accounting. Disk
reclamation additionally depends on ending every applicable lifetime and actual
removal outcomes, not a compression setting or lower replacement-file count.

### Enabled defaults, duplicate packages and build footprint

The host production graph confirms these are dependency decisions, not unused
runtime algorithms to replace:

| Candidate | Verified graph evidence and decision |
| --- | --- |
| Arrow CSV/JSON and Futures executor defaults | DataFusion declares Arrow with defaults plus prettyprint/time-zone support and Futures with defaults; Iceberg also enables Futures defaults. Disabling only DocSpec's direct defaults does not remove those packages/features. Keep current declarations unless a compatible upstream graph reduction is demonstrated. |
| Parquet codec/encryption defaults | DataFusion enables Parquet defaults plus async/object-store; Iceberg enables defaults plus encryption. Preserve input codec compatibility and the current selected Zstandard output. Root-default trimming does not remove upstream requirements. |
| Axum `form`, `query`, `matched-path`, `original-uri` | Current router imports none of those extractors. These are a plausible narrowly scoped build cleanup because DocSpec is the sole Axum parent. However, `serde_urlencoded` remains required by Reqwest and SQLx SQLite, while JSON retains `serde_path_to_error`. Feature-code reduction is possible; package-count or binary-size savings are unmeasured. Lowest priority: resolve the actual changed graph and run transport checks before accepting a narrower declaration. |
| SQLx optional database backends | MySQL/Postgres appear in lock/metadata resolution, but the host normal/build tree has no path to them. Current direct features are already `default-features=false`, Tokio, SQLite and Any; the maintained SQL catalog needs Any. Do not claim unused database binaries or remove required native catalog features. |
| `zstd` 0.13.3 and 0.14.0; `zstd-safe` 7.3.0 and 8.0.0 | Avro/Arrow/DataFusion/Iceberg/Parquet use 0.13; async compression's `compression-codecs` uses 0.14. Both resolve the same `zstd-sys` 2.1.0+zstd.1.5.7. Duplicate Rust wrappers do not establish duplicated native compression or duplicate data work. Defer coordinated upgrades to measured build pressure. |
| Other host duplicate families | `base64`, `darling`, `darling_core`, `darling_macro`, `foldhash`, `getrandom`, `hashbrown`, `itertools`, `miniz_oxide`, `ordered-float`, `rand_core` and `syn` have different upstream consumers/requirements. Host/build contexts also show `memchr` twice at the same version. Do not force incompatible overrides or count each tree entry as a separate release-binary payload. Exact feature/target build cost needs a build measurement. |

The remaining material transitives are accounted for in the earlier matrix:
Arrow/DataFusion component crates own buffers, kernels and plans; Futures/Tokio
utilities own stream/runtime glue; Bytes/HTTP/Hyper own framing; `object_store`
and native Iceberg storage own I/O; Moka owns native caching; SQLx/SQLite own
catalog transactions/backup; codec, allocator, parser and proc-macro/build crates
remain maintained upstream facilities. This refresh found no alternate
project-owned implementation of those facilities to replace.

[Cargo's additive feature rules](https://doc.rust-lang.org/cargo/reference/features.html#feature-unification)
explain why manifest simplification alone need not reduce compiled work. Keep
the existing test-target grouping, development debug/incremental settings and
limited release debug symbols. Any future build claim must measure clean build
time, peak additional target storage and retained artifacts separately from data,
scratch and lifecycle history. Current DataFusion/Arrow upgrade candidates remain
the compatibility work described in the [version research](datafusion-performance-research-2026-10-10.md),
not pinned capabilities or permission for a default flip. No upgrade, allocator
change, custom scheduler, cache, catalog, decoder or join engine is recommended.

## Earlier audit and qualification snapshots

This is a static audit dated 2026-10-10, refreshed against base `0011f7588ab58171c0f3d71e829b4902215ab70b` and the hash-only JSON/comparison-normalization changes. The original audit at `5508a1144d872e4f72ed636eab02dc0eedc0146c` supplied the package research; subsequent refreshes reuse it and check coverage and changed call sites. The comparison findings distinguish the initially inspected implementation, the maintained replacement, and its scoped root-owned qualification. The audit subagent changed only this document and ran no builds, tests, formatting, benchmarks, downloads of data, or corpus-payload reads. Completed checks are attributed to retained root-task receipts. Performance statements distinguish structural effects from timing hypotheses.

The root's [current qualification record](STATUS.md#exact-streamed-endpoint-comparison) includes formatting, strict Clippy and all executable suites in the [latest gate](reviews/dependency-normalization-full-check-2-2026-10-10.log), plus the passing [documentation continuation](reviews/dependency-normalization-doc-check-2026-10-10.log) after missing compiled dependencies were rebuilt. The [normalization review](reviews/comparison-normalization-review-2026-10-10.md) and [hash-only JSON review](reviews/hash-json-implementation-review-2026-10-10.md) approve the scoped changes. Earlier foundation evidence remains retained. These checks establish tested behavior, not a throughput improvement or complete lifecycle qualification.

## Scope and evidence

Coverage includes every direct production and development dependency in [Cargo.toml](../Cargo.toml), the production Python adapter and its build dependency in [pyproject.toml](../adapters/dagster/pyproject.toml), and third-party packages directly imported by the retained experiment scripts. The transitive-package section covers APIs called through re-exports and dependencies materially affecting these paths. This is not a line-by-line audit of every implementation in Cargo.lock or every transitive Python distribution.

Versions and complete resolved Rust feature sets came from `cargo metadata --offline --locked --format-version 1`, checked against [Cargo.lock](../Cargo.lock) and the exact installed crate sources under `/Users/mikewolfd/.cargo/registry/src/index.crates.io-1949cf8c6b5b557f/`. That command resolves metadata without compiling or downloading. The appendix records the resolved graph, including development dependencies; it is not a release-binary size measurement. The root `test-util` feature adds qualification hooks, not another dependency.

Manifest identities at inspection:

| Input | SHA-256 |
| --- | --- |
| `Cargo.toml` | `0424923de3239919fdeab8c18637344cc0fac00fc812fd4184886512f92d7a12` |
| `Cargo.lock` | `1a42088d740c961f0e4261c46bf60e2efebaa5cda5d61caecd8c5de2a5cc906f` |
| `adapters/dagster/pyproject.toml` | `45bdcdb17448c397e0309847814702aa3d9ca035c0b4eb1bb1c4d8ee2138c478` |

The refresh found no omitted direct package: every Rust dependency and dev-dependency has a disposition below, as do the adapter/build dependencies and third-party imports found by syntax-tree inspection of adapter, experiment and script Python files. Local helper imports are project code. The manifest identities above were recomputed and are unchanged, so the previous locked feature inventory is retained rather than regenerated. Cargo.lock versions were checked again against every direct manifest entry. There is no project-owned Rust build dependency; native build helpers and the toolchain are considered separately below. This is coverage of the application’s use, not a claim that every package feature is beneficial or that all transitive implementations were reviewed.

The [existing DataFusion research](datafusion-performance-research-2026-10-10.md), [native column-selection design](reviews/native-column-selection-design-review.md) and [implementation review](reviews/native-column-selection-implementation-review.md) remain the detailed sources for execution planning and selection. This audit verified their main package seams against current code; it does not repeat their proposed experiment. The [retained full local acceptance receipt](../experiments/comments-e2e/acceptance-oracle-threads4-20261010.json) records passing fresh runs. Those existing measurements do not measure any recommendation in this audit.

Exact-version crate source is the authority for pinned behavior. Links below point to tagged upstream source or package-owned API documentation. Current Dagster, Pydantic, Cargo and SQLite documentation supplies general API guidance; it is not evidence that a different version was installed. The inspected Dagster environment contains Dagster 1.13.16 and Pydantic 2.14.0, verified from distribution metadata and source. Python experiment versions come from the retained acceptance receipt. Tagged PyArrow 23.0.0, PyIceberg 0.12.0, DuckDB 1.4.4 and Python blake3 1.0.11 sources were also inspected for the APIs discussed below. No acceptance environment or workload was executed by this audit.

## Recommended work, in order

| Priority and decision | Existing primitive and structural effect | Effort / risk | Next controlled check |
| --- | --- | --- | --- |
| **Implemented: replace the new comparison's unbounded window.** | Pinned DataFusion's whole-partition `SUM` window retains batches outside its memory pool. Grouped signed counts now cancel exact occurrences first. Occurrence streams need no sort or window; value streams use maintained sorted `ROW_NUMBER`, grouped signed counts and `MAX` locators on the remainder. Neither path rescans endpoints or uses a custom merge. | Medium: the final adapter enforces sorted causal ranking and checks incoming rows/bytes; process-memory and disk scaling remain unqualified. | The retained plan, duplicate-heavy fixture, exhausted-pool error, wide sorted-batch refusal and independent value/address fixtures pass. Measure whole-process memory, spill and peak disk before a population-scale claim. |
| **Delivered: native column selection; measurement remains.** | `TableProvider::scan` → Iceberg field-ID selection → Arrow column handles now serves ordinary, located and captured readers. SQL damaged-page fixtures establish that unrequested payload columns can be skipped. This does not necessarily reduce rows scanned. | Semantic implementation reviewed and full gate passed; nested-leaf selection and complete process-memory bounds are not established. | Capture the existing native `ScanMetrics` and run the [bounded protocol](reviews/native-column-selection-design-review.md) before claiming bytes, memory or elapsed-time improvement. |
| **Next: reuse captured metadata within one immutable operation.** | Pass shared `TableMetadataRef`, `Table` and existing `Arc<OccurrenceScan>` values. Qualify reading, hashing and parsing uncompressed metadata once. For the same uncompressed metadata of size M opened K times, one shared read can replace approximately 2KM input bytes with M, excluding manifests and validation at separate boundaries; compressed metadata needs its existing decoding behavior preserved. | Medium / medium: exact identity, pin checks, reservation lifetime and independent verification must remain explicit. | Count metadata opens, bytes, manifest planning calls and retained allocations on unchanged and multi-group paths. Hold payload and history fixed, then grow history separately. |
| **Next: remove the population scan from the common unique-side cardinality proof.** | DataFusion grouping/duplicate checks already provide the needed relational operations. If the constrained side has no duplicate keys, skip its semi join against the large side. This can remove one large-side scan per successful constrained-side proof. | Medium / medium: unmatched duplicates are legal; null modes and composite keys must agree with the join. | Compare unique, matched-duplicate and unmatched-duplicate cases. Retain the current matched-key proof when duplicates exist; measure the extra small-side grouping in that branch. |
| **Then: qualify native row-group layout separately from batch size.** | Parquet already exposes row and estimated encoded-byte limits; the flat export already uses `ArrowWriter::memory_size` and `flush`. Native output still caps each row group at `batch_rows`. A larger qualified row group reduces footer entries from roughly N/B to N/G when file boundaries permit. | Medium / medium-high: larger groups can increase writer memory and the next reader's selected-page memory. | Compare native output bytes, footer size, write/read time, requested scan bytes, peak memory, peak additional disk and retained disk. First qualify a large-row-group/small-batch read; do not borrow the flat-writer result as native proof. |
| **Delivered: maintained hash-only reader.** | `storage::digest` uses `blake3::Hasher::update_reader` and `count`. The pinned implementation uses a 64 KiB stack buffer instead of the previous 1 MiB heap allocation and retries interrupted reads. | Interruption retry is an intentional behavior change; other I/O failures still refuse without returning a partial identity. | [Independent review](reviews/hash-reader-review-2026-10-10.md) approves the change; the [root-owned repository gate](reviews/hash-reader-check-2026-10-10.log) passes, including empty/multi-buffer inputs, short/interrupted reads and propagated errors. No throughput or process-memory improvement is measured. |
| **Delivered: serialize hash-only JSON directly to BLAKE3.** | `serde_json::to_writer(&mut hasher, value)` uses BLAKE3's maintained `Write` implementation. It avoids one serialized byte vector of size J where those bytes are immediately discarded after hashing. | Small / low: compact bytes, map ordering and existing admission are preserved. | [Old/new digest checks](reviews/hash-json-targeted-lib-2026-10-10.log) pass typed requests, Unicode, nested fields and serialization errors; [independent review](reviews/hash-json-implementation-review-2026-10-10.md) approves. Persistence and size-admission buffers remain. No measured speedup. |
| **Defer concurrency and range tuning until unused work is removed.** | Iceberg already has file/range concurrency, range coalescing and footer-prefetch settings; Tokio and DataFusion already own scheduling/admission. | Small code change / potentially high resource impact. | Measure the actual bottleneck and live reader allocation first; vary one setting using the same selected files. Remote request tuning waits for the actual remote adapter. |
| **Defer broad feature trimming and upgrades.** | Cargo features unify across consumers. Disabling a direct default does not remove a feature enabled by DataFusion or Iceberg. Upgrades must keep a compatible Arrow family. | Small-medium / behavior and build risk. | Show an actual resolved-graph reduction, preserve supported SQL/functions/codecs, then measure clean build time and artifacts in a separate authorized experiment. |

The table separates delivered integration from future choices. The audit itself delivers no code changes or measured speedup. Native column selection has a [passing repository gate](reviews/native-column-selection-full-check-3.log), but its controlled performance comparison remains outstanding.

## Every direct Rust dependency

The feature appendix gives the complete effective set. This table distinguishes capabilities already used from useful replacements and deliberate limits.

| Package / pinned version | Current use and maintained functionality already used | Recommendation |
| --- | --- | --- |
| [anyhow 1.0.104](https://docs.rs/anyhow/1.0.104/anyhow/) | [CLI](../src/main.rs#L263) uses `Result`, `bail!`, `ensure!` and automatic source conversion. Library [Error](../src/error.rs) preserves actionable refusal codes across DataFusion causes. Default `std`. | **Keep.** A blanket conversion of library errors to anyhow would lose useful stable codes. Extra error-framework machinery has no demonstrated performance value. |
| [arrow 58.4.0](https://github.com/apache/arrow-rs/tree/58.4.0/arrow) | Typed arrays, schemas, record batches, compute casts and IPC. [Batch projection](../src/storage/identity.rs#L38), [writer normalization](../src/storage.rs#L312) and [HTTP IPC](../src/http/ipc.rs#L1) share array handles or stream batches. Defaults include CSV/JSON/IPC; DataFusion also activates pretty printing and time-zone support. | **Keep; native selection now uses the maintained reader.** Preserve buffer sharing. Do not equate `Arc`/array-handle clones with copying values. String views are a measured candidate only where current Utf8 schema/downcasts and downstream readers can accept them; no blanket conversion. |
| [async-trait 0.1.92](https://docs.rs/async-trait/0.1.92/async_trait/) | Implements Iceberg `Catalog` and DataFusion `TableProvider` interfaces in [catalog.rs](../src/catalog.rs#L558) and [query.rs](../src/query.rs#L129). It supplies boxed async trait futures required by those dependency interfaces. No features. | **Keep.** Replacing it locally with native async traits does not make those upstream dynamic interfaces object-safe or remove their expected signatures. Avoid spreading it into ordinary internal functions unnecessarily. |
| [axum 0.8.9](https://docs.rs/axum/0.8.9/axum/) | [Router](../src/http.rs#L32), typed state/JSON/path extraction, maintained request body limit, errors and `Body::from_stream`; graceful shutdown in [main](../src/main.rs). Uses default features. | **Keep.** HTTP transport is thin. `form`, `query`, `matched-path` and `original-uri` are potential unused direct defaults, but trim only with a verified graph reduction. Do not add queues, automatic mutation retries or transport timeouts that imply cancellation of durable work. |
| [blake3 1.8.7](https://docs.rs/blake3/1.8.7/blake3/struct.Hasher.html) | [Single-pass admission copy/hash](../src/storage.rs#L454), [flat export hash while writing](../src/engine/export.rs#L810), byte identity and compact request hashes. `std`; no `rayon` or `mmap`. | **Keep; hash-only reader and JSON replacements are delivered.** Current copy/hash remains justified because it creates durable bytes and checks identity in one pass. Parallel/mapped hashing is not automatically faster and introduces another resource/concurrency choice. |
| [clap 4.6.7](https://docs.rs/clap/4.6.7/clap/) | [CLI](../src/main.rs#L29) uses derive-based parsing, subcommands and maintained help/validation. Default human-facing features plus `derive`. | **Keep.** No custom argument parser to replace. Completion generation or color trimming is a product/build decision, not a pipeline optimization. |
| [datafusion 53.1.0](https://github.com/apache/datafusion/tree/53.1.0) | [Shared runtime](../src/budget.rs#L85), spill-capable participating operators, streaming execution, native physical statistics, joins, aggregation, scalar expressions and maintained expression builders. [Write helpers](../src/engine/write.rs#L640) avoid generated SQL in some internal joins. Broad default SQL/function support. | **Keep; occurrence comparison now avoids ranking, and value ranking has explicit admission. Eliminate provable redundant relational work next.** A runtime pool does not account for every operator: see the window finding below. Preserve both pinned optimizer guards. Migrate remaining generated SQL to maintained expressions only where it simplifies actual code or addresses demonstrated parser stack pressure. Do not implement a join engine, key index or SQL backend. |
| [fs2 0.4.3](https://docs.rs/fs2/0.4.3/fs2/) | OS-held workspace lock in [budget](../src/budget.rs#L174), free-space checks before output/restore. No features. | **Keep.** Rust's standard `File` now has locking APIs, but available-space queries still use fs2; replacing only the lock does not remove the dependency. Existing process-death semantics are valuable. File preallocation would change disk behavior and has no measured benefit here. |
| [futures 0.3.34](https://docs.rs/futures/0.3.34/futures/) | `Stream`, `TryStreamExt`, `try_unfold`, `try_flatten`, boxed streams/futures and pinning across [storage](../src/storage.rs), [query](../src/query.rs) and [IPC](../src/http/ipc.rs). Defaults enable executor utilities although Tokio runs the application. | **Keep.** Demand-driven streams already provide backpressure. Use bounded combinators only when independent work merits concurrency. Removing the direct `executor` default alone cannot eliminate it when other dependencies enable defaults. |
| [iceberg 0.10.1](https://github.com/apache/iceberg-rust/tree/v0.10.1/crates/iceberg) | Maintained catalog trait, schema/field IDs, snapshots, manifests, Arrow reader, Parquet/rolling writer, transaction actions and references. [Storage](../src/storage.rs), [schema adapter](../src/storage/schema.rs), [occurrence adapter](../src/storage/occurrences.rs). Empty default feature set; concrete local storage is explicitly injected. | **Keep; field selection is implemented, while metrics retention and broader shared metadata reuse remain.** Retain selected-schema/default/occurrence adaptation and application retention authority. Do not enable native expiration or pruning merely because APIs exist. Current all-history inventory work and double metadata opens are integration opportunities, not a reason to replace the library. |
| [iceberg-catalog-sql 0.10.1](https://github.com/apache/iceberg-rust/tree/v0.10.1/crates/catalog/sql) | [Local catalog](../src/storage.rs#L58) uses `SqlCatalogBuilder`, QMark binding and `LocalFsStorageFactory`. SQL stores native catalog bookkeeping; lifecycle rows remain Iceberg data. No crate features. | **Keep.** Maintained pool/transactions and conditional catalog update avoid a custom catalog. Pool sizing is configurable already; current default is 10 connections, not evidence that 10 concurrent writers improve SQLite. Tune only from contention evidence; preserve durability and guarded selection. |
| [mimalloc 0.1.52](https://docs.rs/mimalloc/0.1.52/mimalloc/) | Global allocator selected in [CLI main](../src/main.rs#L26). Default has no optional flags. | **Keep.** This applies to the CLI binary; a library embedder chooses its own allocator. There is no measured reason for alternate allocator flags or allocator replacement. Maintain release debug symbols for profiling. |
| [parquet 58.4.0](https://github.com/apache/arrow-rs/tree/58.4.0/parquet) | Arrow writer, footer parser, field IDs, Zstandard, bounded/truncated statistics, disabled offset indexes, flat writer memory checks. [Native writer](../src/storage.rs#L223), [flat writer](../src/engine/export.rs#L438), [inspection](../src/storage/inspection.rs). Default codecs; transitive async/object-store/encryption support. | **Keep; qualify native row-group layout.** Reuse writer/reader size and footer APIs instead of hand-encoding Parquet. Page indexes/Bloom filters help only when actual consumers use qualified predicates; generating them now could add retained bytes without skipping reads. Keep input codec compatibility. |
| [serde 1.0.229](https://docs.rs/serde/1.0.229/serde/) | Derived typed metadata and request serialization throughout [model](../src/model.rs), [export](../src/export.rs), [restore](../src/restore.rs). `derive`, `std`; graph also enables `rc`/`alloc`. | **Keep.** This already replaces handwritten model encoding. Borrowed deserialization is only useful where ownership/lifetime permits it; retained operations and async work generally need owned data. Avoid a parallel schema/serialization framework. |
| [serde_json 1.0.151](https://docs.rs/serde_json/1.0.151/serde_json/) | Typed metadata, lifecycle JSON and CLI/HTTP output. [Bounded metadata sink](../src/storage/tables.rs#L95) and [counting sink](../src/engine/inputs.rs#L484) already use `to_writer`. `raw_value` is enabled transitively. | **Keep; hash-only serialization now streams into BLAKE3.** The bounded sink remains necessary because ordinary `to_vec` does not impose the application byte allowance. Use typed `get` for a single lifecycle record rather than decoding every record. RawValue is not needed for values the application must validate. |
| [sqlx 0.8.6](https://docs.rs/sqlx/0.8.6/sqlx/) | Native catalog activates Any+SQLite; [restore](../src/engine/restore.rs#L777) uses typed SQLite connections, bound queries, `VACUUM INTO`, explicit close and integrity check. Direct defaults disabled; runtime-tokio/sqlite/any enabled. | **Keep.** Already uses maintained consistent SQLite copy, not byte-copying a live database. Backup API can be incremental, but introducing another driver to obtain it needs measured catalog-copy pressure. `Any` is required by the native catalog even if our restore code uses SQLite directly. |
| [tokio 1.53.2](https://docs.rs/tokio/1.53.2/tokio/) | Async runtime/network/signal handling, dataset mutexes, owned semaphores, `spawn_blocking`, cooperative yields and task recovery. [Budget](../src/budget.rs#L129) retains worker-owned permits after caller cancellation. | **Keep; align settings before adding parallelism.** `compute_threads` controls DataFusion partitions and admitted blocking jobs, while CLI worker threads are separately fixed at 12. Tokio's blocking pool is another execution resource. Preserve cancellation accounting; no custom scheduler or unbounded spawned work. |
| [tracing 0.1.44](https://docs.rs/tracing/0.1.44/tracing/) | Structured phase/failure events and capacity/lock diagnostics; [budget warning](../src/budget.rs#L205) and engine phase sites. Defaults include attributes; graph includes log compatibility. | **Keep.** Use existing structured fields for measured phase counters. No new tracing backend or per-row events. Attribute macros can be trimmed only if unused across the resolved graph. |
| [tracing-subscriber 0.3.23](https://docs.rs/tracing-subscriber/0.3.23/tracing_subscriber/) | [CLI setup](../src/main.rs#L263) uses `fmt` with `EnvFilter` and stderr. Default formatting/registry/ANSI/log support plus env-filter. | **Keep.** Environment filtering provides useful optional detail. A lighter `Targets` filter is available but changes filtering behavior; no evidence warrants that tradeoff. JSON logging is not required for current output. |
| [uuid 1.27.0](https://docs.rs/uuid/1.27.0/uuid/) | Random object/table/operation identifiers and parsing; [storage](../src/storage.rs#L240), [restore](../src/restore.rs#L331), removal validation. Direct v4/serde; Iceberg/DataFusion add v7/js. | **Keep.** Random IDs avoid a custom allocator. Switching to v7 does not improve a file/object namespace without a measured locality problem, and would not replace stable caller retry identities. |
| [tempfile 3.27.0](https://docs.rs/tempfile/3.27.0/tempfile/) — dev | `TempDir`/`tempdir` isolate tests and remove disposable fixtures on drop. Default secure-name generation. | **Keep.** Appropriate for tests. Do not replace durable candidate/evidence directories with automatic temporary cleanup; their recovery lifetime differs. No production throughput impact from this direct dev use. |
| [tower 0.5.3](https://docs.rs/tower/0.5.3/tower/) — dev direct | `ServiceExt::oneshot` exercises the actual Axum router in transport tests; `util` explicitly enabled. Axum and other runtime dependencies also use Tower transitively. | **Keep.** Maintained service test utilities already avoid a custom HTTP harness in Rust. The unified graph's retry/timeout features are not proof that DocSpec applies retry middleware. Add runtime middleware only for a concrete transport requirement. |

## Native reader, metadata and writer findings

### Selection is implemented; measure through existing native metrics

[NativeTable::scan](../src/query.rs#L157) now composes requested indices into the native source before constructing its plan. [NativePartition::execute](../src/query.rs#L317) opens selected ordinary, located or captured readers through the shared [selection adapter](../src/storage/selection.rs). Maintained Iceberg field-ID selection and Arrow array handles preserve exact order, repeated columns, empty-output row counts, defaults and stored-origin requirements. The [implementation review](reviews/native-column-selection-implementation-review.md) records the passed semantic and repository checks, including actual SQL queries over a deliberately damaged unused column and a sparse Struct root. This establishes selected-column behavior, not a measured end-to-end speedup or arbitrary nested-leaf pruning.

Keep filters above occurrence readers: physical row positions are formed before filtering, and pinned Iceberg does not provide the required `_pos` replacement. Empty/all-absent physical selections can still trigger a full physical-column fallback. Retain row statistics, the filtered-count optimizer guard and null-safe dynamic-join guard. More enabled pruning is not automatically correct.

The pinned [Iceberg scan result](https://github.com/apache/iceberg-rust/blob/v0.10.1/crates/iceberg/src/arrow/scan_metrics.rs) already exposes clonable `ScanMetrics`; current readers immediately discard it by taking `.stream()`. Keep that handle in the qualification path. Its byte counter records requested ranges before I/O and excludes planning traffic. It is not physical disk traffic, a successful-delivery total, or a complete metadata-request count. No second counting decoder is needed.

### Reuse handles and captured inventories; do not add a general cache

[open_pin](../src/storage.rs#L677) calls `verify_object`, which opens and hashes the metadata file, then `StaticTable::from_metadata_file`, which reopens and parses it. For uncompressed JSON, a bounded read can establish the digest/length and deserialize the **same** bytes with `serde_json`, then construct a read-only table with [TableBuilder::metadata](https://github.com/apache/iceberg-rust/blob/v0.10.1/crates/iceberg/src/table.rs#L74), `metadata_location`, explicit storage/runtime and the existing snapshot/schema checks. That removes a duplicate file read per such open. Preserve size admission before allocation, error propagation and worker-owned reservations; existing [origin checks](../src/engine/backfill/origins/checks.rs#L223) demonstrate admitted immutable metadata reuse.

An unconditional `serde_json::from_slice` replacement would change behavior: pinned [TableMetadata::read_from](https://github.com/apache/iceberg-rust/blob/v0.10.1/crates/iceberg/src/spec/table_metadata.rs#L450) recognizes gzip bytes and decompresses them first. Its codec helper is crate-private and materializes decoded bytes. Keep the existing native path for compressed metadata until a maintained decoding path and its decoded-size admission are qualified. Reusing already opened handles avoids repeated work without introducing a new decoder. Include compressed metadata in any later equivalence check; encoded file size alone does not bound decoded memory.

There is an even smaller allocation correction in the inspected origin-check code: its `table` method obtains `Arc<TableMetadata>` but passes `(*metadata).clone()` to the builder. The maintained setter accepts `Into<TableMetadataRef>`, so passing the existing Arc shares metadata instead of deep-cloning it. This saves a copy proportional to that metadata structure on every such construction. The parent task has been informed; this report does not change that active feature's files or claim the correction is applied.

Reuse the resulting immutable `Table` or captured `Arc<OccurrenceScan>` within a request before adding another cache. [Occurrence opening](../src/storage/occurrences.rs#L87) first invokes [snapshot_files](../src/storage.rs#L480), whose purpose includes retaining paths from **every snapshot in the selected metadata**, then plans native file tasks again. Separate cardinality and main queries can repeat this work. If K executions repeat an inventory containing H snapshots and M unique manifests, captured reuse can reduce K full inventory passes to one; the exact byte/request effect depends on shared manifests. Full retention closure remains necessary at retention/admission boundaries. A scan-only current-file inventory is a separate potential simplification, not permission to remove historical protection.

The library's table clone shares an existing object cache, but the manual `manifest_list_reader(...).load()` and `entry.load_manifest(...)` calls in `snapshot_files` do not go through that cache. Merely retaining a `Table`, or increasing DataFusion's metadata cache, therefore does not remove these direct inventory loads. Reusing the already admitted captured inventory is the clearer first change.

The [pinned native cache](https://github.com/apache/iceberg-rust/blob/v0.10.1/crates/iceberg/src/io/object_cache.rs#L29) has a default configured capacity of 32 MiB, but its weight uses `size_of_val` on the `Manifest`/`ManifestList` structs. Their heap-owned entries are not included by that expression. Treat that value as cache weight, **not** a measured heap bound. New long-lived handle reuse must keep a bounded lifetime and account for retained metadata; changing `cache_size_bytes` alone does not prove a complete memory ceiling. No new cache implementation is recommended by this observation.

### Writer configuration is useful only together with reader memory evidence

[Native writing](../src/storage.rs#L223) uses the maintained Iceberg `ParquetWriterBuilder`, rolling writer and unpartitioned writer. It enables Zstandard, chunk statistics truncated to 64 bytes and disables offset indexes; periodic close bounds accumulated footer descriptors. The row-group row count is still `batch_rows`. The [Parquet properties builder](https://github.com/apache/arrow-rs/blob/58.4.0/parquet/src/file/properties.rs#L731) already separates row-group rows and estimated encoded bytes from input batches.

The [flat exporter](../src/engine/export.rs#L438) now retains larger groups and flushes from `ArrowWriter::memory_size()`, while hashing bytes through its output sink. Keep that successful design. Iceberg's file writer exposes `current_written_size`, including buffered encoded size, but this is not the same as full Arrow-writer memory. Increasing the native row-group size needs revised admission/metadata accounting, not just a larger constant. Parquet's async reader can buffer selected row-group pages despite returning small batches. Neither a `batch_rows` setting nor an encoded-byte target is a complete process-memory bound.

Do not add page indexes, Bloom filters, dictionaries or partitioning globally. The present occurrence path deliberately rejects filtering; a newly written index cannot help that path yet. Per-column encoding remains a maintained Parquet option when measured repeated-value columns justify it. Preserve the exact independent reader and inspect files/footers using existing qualification facilities.

Track storage amplification separately from reader improvements. Column selection can avoid decoding without reducing stored files. Any row-group or write-path comparison must include native and flat output, spills, metadata, retained prior versions, failed attempts and temporary old/new coexistence; report peak additional disk and retained bytes against the same input baseline. Build caches remain a separate cost. Reuse existing file/size inventories instead of rescanning payloads to produce this account.

Native creation still performs an initial digest pass because the maintained native writer does not return a DocSpec content hash. Removing it would require a storage/writer adapter. That is a documented exception to doing work once; the flat writer's existing hashing sink shows the simpler case. A new generic hashing storage layer is deferred until native digest time is measured. Never remove independent qualification hashing or reinterpret its trust boundary as wasted work.

### Small maintained replacements without a new abstraction

The implemented [digest reader](../src/storage.rs#L400) delegates the hash-only loop to [BLAKE3’s `update_reader` and `count`](https://docs.rs/blake3/1.8.7/blake3/struct.Hasher.html#method.update_reader). Its pinned `src/io.rs::copy_wide` uses a 65,536-byte stack buffer and retries `Interrupted`; that buffer size is an implementation detail, not a stable API guarantee. The replacement removes the custom heap buffer and counter. It does not remove a file read, and this audit supplies no timing result. Admission copy/hash and output hashing still serve different, justified write boundaries. The [independent review](reviews/hash-reader-review-2026-10-10.md) and [passing root-owned gate](reviews/hash-reader-check-2026-10-10.log) establish the checked behavior, not a performance result.

Hash-only JSON now uses [`storage::json_digest`](../src/storage.rs#L56) at the audited request, dependency, phase and owner-protection call sites. [`serde_json::to_writer`](https://docs.rs/serde_json/1.0.151/serde_json/fn.to_writer.html) serializes the same compact bytes directly into BLAKE3’s maintained `Write` implementation. The helper returns a digest only after successful serialization. The [implementation review](reviews/hash-json-implementation-review-2026-10-10.md) traces every migrated input and retained admission rule; the [focused checks](reviews/hash-json-targeted-lib-2026-10-10.log) compare the prior buffered path, frozen typed requests, nested/Unicode values and partial errors. Buffers also used for persistence or size checks remain. This removes one complete temporary JSON vector at each migrated call site; throughput and process-memory effects are unmeasured.

### Relational work and parser pressure

[join_queries](../src/engine/refresh.rs#L643) validates many-to-one/one-to-one cardinality by semi-joining candidate keys against the other input before grouping duplicates. DataFusion already supplies grouping, semi joins and expression construction. The unique-side shortcut in the decision table is an application inference; it is not an upstream optimizer guarantee. Duplicates that never match must remain legal. Do not materialize a durable key index or cache the whole relation to avoid this one scan.

The earlier feature gate retained a [default-stack failure trace](reviews/dependency-correction-stack-backtrace-full.log) in sqlparser's `parse_expr_prefix_by_reserved_word` under generated incremental-join SQL. A later selection gate also reproduced stack exhaustion; boxing the shared native file-planning future was followed by a passing normal-stack fixture and [full gate](reviews/native-column-selection-full-check-3.log), as recorded in the [implementation review](reviews/native-column-selection-implementation-review.md). This supports checking parent async-frame size and replacing fragile internal generated SQL with maintained DataFusion expressions where simpler. It does **not** establish a general sqlparser defect, a need to raise all stack limits, or failure of its entire recursion guard.

### New comparison: use maintained operators with verified memory behavior

The initially inspected [comparison stream](../src/engine/comparison.rs) combined endpoint rows once, then used a whole-partition `SUM(weight)` window and ranked each side. Its statement that the shared DataFusion pool owned and refused all window state was incorrect for the pinned version. This is a concrete resource finding in the active change, not a measured failure of the previously accepted Comments workload.

The exact upstream path explains why. With no ordering or explicit frame, `WindowFrame::new(None)` ends at unbounded following. `PlainAggregateWindowExpr::uses_bounded_memory` then returns false, and the planner selects `WindowAggExec`. Its `WindowAggStream` pushes input batches into a vector, concatenates the entire physical partition at EOF, then evaluates the logical groups. It has no `MemoryReservation`, capacity growth check, or spill path. A small logical duplicate group therefore does not bound that operator's retained input. An output-size check runs too late to prevent this allocation. These are pinned-source observations; inspect the final optimized plan before attributing a running query's actual operator choice. [Default frame](https://github.com/apache/datafusion/blob/53.1.0/datafusion/expr/src/window_frame.rs#L146), [planner selection](https://github.com/apache/datafusion/blob/53.1.0/datafusion/core/src/physical_planner.rs#L884), [unbounded executor](https://github.com/apache/datafusion/blob/53.1.0/datafusion/physical-plan/src/windows/window_agg_exec.rs#L322).

The maintained replacement preserves a single endpoint scan: union the sides and first sum signed weights by typed values plus exact file/row occurrence. Discard zero totals. Occurrence streams emit the remainder without sorting or ranking. Value streams assign `ROW_NUMBER` to those remaining occurrences, partitioned by value plus side and ordered by file/row, then group on value plus ordinal. Sum signed weights and discard zero totals. Each ordinal has at most one member on each side; a surviving group contains exactly one member. `MAX(file)` and `MAX(row)` therefore recover its locator without choosing between different surviving rows. `MAX` uses the pinned vectorized `GroupsAccumulator` for Utf8 and integers; unordered `FIRST_VALUE` falls back to per-group accumulators, and even ordered `FIRST_VALUE` does not provide that grouped implementation for Utf8. This is a specific maintained-feature advantage, not a measured speedup. No extra endpoint scan, durable key index, or custom merge/window is needed. [MAX implementation](https://github.com/apache/datafusion/blob/53.1.0/datafusion/functions-aggregate/src/min_max.rs#L228), [FIRST_VALUE implementation](https://github.com/apache/datafusion/blob/53.1.0/datafusion/functions-aggregate/src/first_last.rs#L181). Exact-occurrence cancellation also prevents intermediate addresses from surviving future change composition. The [normalization review](reviews/comparison-normalization-review-2026-10-10.md) traces the required endpoint uniqueness invariant; the public planner still selects full/unchanged paths, so the signed-chain unit fixture is algebra evidence rather than qualified chain admission.

`ROW_NUMBER` itself is causal, advertises bounded execution and retains a counter. Its stateful evaluator only needs the current-row range. With fully sorted partition keys, `BoundedWindowAggStream` emits each batch, removes ended groups, and prunes an ongoing group's old rows. It can retain a final row slice backed by a previous batch and creates per-batch key/result state; it does not retain the whole giant duplicate group in this sorted mode. The operator still lacks a memory-pool reservation. Require an explicit allowance for its live batches/state, capture `BoundedWindowAggExec` with `mode=[Sorted]`, and account for every active execution partition. A partial/linear ordering can retain more open groups and is not covered by this conclusion. [ROW_NUMBER evaluator](https://github.com/apache/datafusion/blob/53.1.0/datafusion/functions-window/src/row_number.rs#L131), [stateful standard window](https://github.com/apache/datafusion/blob/53.1.0/datafusion/physical-expr/src/window/standard.rs#L149), [buffer pruning](https://github.com/apache/datafusion/blob/53.1.0/datafusion/physical-plan/src/windows/bounded_window_agg_exec.rs#L1086).

The follow-up review found that a fixed allowance based only on source batch bytes is insufficient: `SortExec` emits row-count-sized batches, can combine wide rows from separate inputs, and releases its reservation as it yields. Check the **actual sorted batch immediately before the window**, not only native-source or final-output batches. The pinned APIs have no DataFrame `map_batches` or sort-output byte limit. `BatchSplitStream` slices by row count and retains backing buffers; `BufferExec` adds a queue and releases its reservation on delivery. Neither solves this downstream ownership boundary. [Sort handoff](https://github.com/apache/datafusion/blob/53.1.0/datafusion/physical-plan/src/sorts/sort.rs#L703), [batch splitting and reservation stream](https://github.com/apache/datafusion/blob/53.1.0/datafusion/physical-plan/src/stream.rs#L571), [buffer delivery](https://github.com/apache/datafusion/blob/53.1.0/datafusion/physical-plan/src/buffer.rs#L393).

The smallest adapter is a transparent batch check inserted directly below the qualified window in the final physical plan: use maintained tree traversal and `with_new_children`, preserve the child's properties/order/partitioning, and wrap `child.execute` with `RecordBatchStreamAdapter` plus a checked map. Reject a batch exceeding the admitted byte/row allowance before the window retains it; preserve explicit admission for its live batches and key/result bookkeeping until drop. The public `BoundedWindowAggExec::input_order_mode` field supports a typed `Sorted` guard. Avoid a second optimizer pass that might move the check, a new source that hides the upstream tree, row-at-a-time execution, or a custom sorting/window implementation. This adapter is a concrete missing admission boundary, not a new execution framework. The focused fixtures below exercise sorted-output overflow, admission release and ordinary paths; wider cancellation and process-memory qualification remain open.

The implemented replacement now has [physical-plan/resource evidence](reviews/change-stream-window-plan-5.log) and [independent value/address fixtures](reviews/change-stream-targeted-6.log). The byte check sits immediately below the window after physical optimization, preserves child properties and permits only the expected fully sorted `ROW_NUMBER`. Its retained allowance covers batch/tail buffers, owned key-payload copies and row/field bookkeeping. Wide rows that fit individual input batches but exceed the sorted-window allowance explicitly refuse, including when the complete value difference would be empty. A deliberately insufficient DataFusion pool returns a resource error. Admission remains an allocation estimate; these fixtures do not establish arbitrary population-scale RSS bounds. The source trace supports its duplicate-aware cancellation and removes the known whole-partition window; it does not establish the complete query's peak memory, spill cost, elapsed time or disk amplification. Retain the distinction between the efficient count-only aggregation and the address-producing stream. Once both values and addresses are requested, derive counts from that consumed result rather than invoking a second full comparison. Source replacements now preserve origins outside their applied scope through the shared helper. File-level related discovery can still recompute equal-valued groups; no package's physical row position substitutes for an explicit logical identity.

## Re-exported APIs and material transitive packages

These are not additional direct dependencies to add. Their versions/features come from the same resolved graph.

| Package family / version | How it is reached and the efficiency conclusion |
| --- | --- |
| `arrow-array`, `arrow-schema`, `arrow-buffer`, `arrow-data`, `arrow-select`, `arrow-cast` 58.4.0 | Through Arrow re-exports: batch/array/schema handles, casts and kernels. Keep maintained type/validity checks and zero-copy handle projection. `StringArray` construction for repeated origins does copy path values; substituting view/dictionary types would change qualified schemas/downcasts, so profile and qualify before redesigning it. |
| `arrow-ipc` 58.4.0 | Through `arrow::ipc`; compression support is present transitively, but the current writer does not request compression. Keep streaming. HTTP dictionary state is explicitly unqualified and refused; merely allowing dictionary schemas would add retained encoder state outside the current proof. Compression is a remote-byte/CPU tradeoff, not an automatic local improvement. |
| `arrow-json`, `arrow-csv` 58.4.0 | Enabled through Arrow/DataFusion defaults; no direct application CSV/JSON array-reader use found. Metadata JSON is Serde, not Arrow JSON. Disabling the root Arrow defaults alone does not remove these packages because DataFusion also depends on Arrow with defaults. |
| `datafusion-common`, `datafusion-catalog`, `datafusion-datasource`, `datafusion-execution`, `datafusion-expr`, `datafusion-physical-expr`, `datafusion-physical-plan`, `datafusion-physical-optimizer`, `datafusion-sql` 53.1.0 | Called through DataFusion re-exports in budget, query and engine modules. Native `DataSourceExec`, statistics, standard plans and expression builders already avoid custom execution operators. Keep the runtime shared. SQL parsing is appropriate for authored SQL; generated internal expressions need not roundtrip through text. |
| `datafusion-functions-aggregate`, `datafusion-functions-aggregate-common`, `datafusion-functions-window`, `datafusion-functions-window-common` 53.1.0 | Re-exported aggregate/window builders and implementations support the comparison implementation above. Reuse maintained ranking and vectorized grouped aggregates; inspect each selected operator's accounting instead of inferring bounds from the shared runtime or `execute_stream`. No new direct dependencies are needed. |
| `datafusion-datasource-parquet` 53.1.0 | DataFusion's Parquet input reader supports its own pruning/caching. Native Iceberg reads use a different decoder integration. Tuning DataFusion Parquet settings cannot be assumed to tune all native reads. |
| `sqlparser` 0.61.0, `recursive` 0.1.1 | Used by DataFusion SQL, not imported as a new direct parser. `recursive-protection` and visitor support are enabled. Pinned parser source marks selected recursive methods; the live failure must be evaluated with its actual calling stack. Keep protection; no custom parser. |
| `bytes` 1.12.1 | Directly called as Axum's `Bytes` re-export. [IPC](../src/http/ipc.rs#L73) uses `Bytes::from_owner` to transfer its Vec and owned admission permit together. The [maintained implementation](https://docs.rs/bytes/1.12.1/bytes/struct.Bytes.html#method.from_owner) preserves owner lifetime across clones. Keep this; converting to `BytesMut` would copy owner-backed data. |
| `http` 1.5.0, `http-body` 1.1.0, `http-body-util` 0.1.5, `hyper` 1.12.0, `hyper-util` 0.1.21 | Axum re-exports HTTP types and uses maintained body/server machinery. Our only transport adaptation is bounded metadata/errors and Arrow chunks. Keep it; another HTTP implementation or producer channel is unnecessary. Verify Cargo.lock for body version changes rather than freezing this table as a permanent inventory. |
| `object_store` 0.13.2 | DataFusion/Parquet dependency with local `fs` and Tokio features; no direct DocSpec object-store API use. Iceberg 0.10.1's current `FileIO` uses its own explicitly injected `StorageFactory`/`LocalFsStorageFactory`. Do not assume configuring DataFusion's object-store registry configures native Iceberg I/O. Use their maintained storage interfaces for the future remote adapter. |
| `moka` 0.12.16 (`future`) | Owns the native Iceberg metadata cache. Keep the maintained cache, but account for the weight limitation above and request lifetime before widening reuse. No direct Moka dependency or new application LRU is warranted. |
| `sqlx-core`, `sqlx-sqlite` 0.8.6 and `libsqlite3-sys` 0.30.1 | Native catalog/restore storage, with bundled SQLite. Do not remove `any`, bundle settings or runtime features because only some are named by our direct restore calls. Native catalog pooling is already maintained. |
| `libmimalloc-sys` 0.1.49 | Allocator implementation behind mimalloc. No direct unsafe allocator interface use. Keep the current wrapper and platform defaults. |
| `zstd` 0.13.3 / 0.14.0, `zstd-sys` 2.1.0+zstd.1.5.7 and other Parquet codecs | Maintained compression, reached through Parquet/Arrow and other dependencies. Multiple wrapper versions do not prove duplicate payload compression or justify incompatible dependency overrides. Measure build impact separately; retain source codec support. |
| `async-compression` 0.4.50; `reqwest` 0.12.28; Tower runtime features | Pulled in by higher-level components. No project-owned alternate codec, HTTP client or retry middleware was found in Rust. Do not activate or bypass those layers based solely on their availability. Current remote-provider behavior is unqualified. |

The HTTP body version in this table and other resolved values are a dated observation; [Cargo.lock](../Cargo.lock) remains authoritative. The feature appendix is generated from metadata rather than inferred from package descriptions.

## Python production adapter, build and qualification packages

| Package / version status | Current use and decision |
| --- | --- |
| [Dagster](https://dagster.io/docs/api/dagster/ops) `==1.13.16`; no extras requested | [Definitions](../adapters/dagster/src/docspec_dagster/definitions.py) use native `Config`, `ConfigurableResource`, `op`, `job`, output metadata, `RetryRequested` and `Failure(allow_retries=False)`. The installed pinned error boundary processes explicit retry/failure before generic retry policy. **Keep.** Exact original requests are retried only for unknown outcomes; Rust owns durable reuse. A new scheduler/state store would duplicate work. Native assets/partitions/concurrency controls belong to a demonstrated multi-operation workload, not this one-op adapter by default. |
| [Pydantic](https://pydantic.dev/docs/validation/latest/concepts/fields/) `>=2,<3`; inspected adapter environment 2.14.0; no extras requested | `Field` enforces finite positive timeout, response byte range and retry limits in the Dagster resource. **Keep.** A separate validation framework would duplicate it. The stdlib client still needs standalone protocol checks, including duplicate JSON members and exact response identity; reserializing through a model could change the byte-identical retry request. |
| [setuptools](https://setuptools.pypa.io/en/latest/userguide/pyproject_config.html) `>=68`; build only | Uses package discovery and `setuptools.build_meta`. No runtime bulk processing. **Keep.** A backend change has no demonstrated pipeline benefit. No resolved build version is recorded by this manifest; do not invent one. |
| [DuckDB](https://github.com/duckdb/duckdb/blob/v1.4.4/src/include/duckdb/main/settings.hpp) 1.4.4 in accepted qualification receipt; no repository Python lock found | [Oracle](../experiments/comments-e2e/oracle.py#L96) uses SQL views, direct Parquet scanning, exact duplicate-aware comparison, memory/spill controls, four threads and disabled insertion-order preservation. **Keep.** Existing four-thread acceptance is evidence for this workload. Full-value grouping can spill; adding threads is not an assumed improvement. Dictionary encoding of repeated contributor paths is a deferred measured candidate, not a replacement of exact comparison by a hash. Qualification-only, not embedded production runtime. |
| [PyArrow](https://github.com/apache/arrow/blob/apache-arrow-23.0.0/python/pyarrow/parquet/core.py) 23.0.0 in accepted receipt | Oracle reads footers/schemas and constructs typed expectations; [preparation](../experiments/comments-input/prepare.py#L58) streams `iter_batches`, filters arrays and writes Zstandard Parquet. **Keep.** These already use maintained columnar operations. Historical [small local-reuse oracle](../experiments/local-reuse/run.py#L63) calls `read_all`/`to_pylist`/Counter; keep its bounded-fixture scope. If expanded, reuse the existing exact DuckDB comparator instead of building a population-sized Python object set. |
| [PyIceberg](https://github.com/apache/iceberg-python/blob/pyiceberg-0.12.0/pyiceberg/table/__init__.py) 0.12.0 in accepted receipt | Oracle uses `StaticTable`, explicit snapshot `plan_files` and `schema_to_pyarrow`; it deliberately avoids a Python full-table materialization. **Keep.** This independently reads native membership/schema while DuckDB scans payloads. Delete files and partial-task behavior remain explicit refusals in this frozen workload. Native Rust and Python version numbers are independent; do not infer feature parity. |
| [psutil](https://psutil.readthedocs.io/) 7.0.0 in accepted receipt | [Monitor](../experiments/comments-input/run.py#L83) samples process descendants and RSS; system memory is captured in environment metadata. **Keep.** Sampling is a lower bound and distinct from engine memory allowance. `oneshot()` only helps grouped getters sharing a syscall; do not add it without identifying repeated getters. No production dependency. |
| [Python blake3](https://github.com/oconnor663/blake3-py/blob/1.0.11/blake3.pyi) 1.0.11 in accepted receipt | [Integrity helpers](../experiments/comments-e2e/integrity.py#L70) hash BLAKE3 and optional SHA-256 in the same pass while checking file identity before/after. **Keep.** Replacing it with separate convenience whole-file hashes would repeat reads and lose the current mutation check unless carefully reconstructed. Parallel hashing is deferred. |
| [NumPy](https://numpy.org/doc/stable/reference/generated/numpy.arange.html); imported by preparation, exact version not captured in inspected receipt | [prepare.py](../experiments/comments-input/prepare.py#L64) vectorizes the deterministic physical-row mask using `arange` and integer arithmetic over one batch. **Keep.** It already avoids a Python per-row loop. The prepared corpus is retained; rerunning selection merely to use a different API would repeat completed work. Record NumPy's version if that preparer is run again. |

The adapter's [stdlib HTTP client](../adapters/dagster/src/docspec_dagster/client.py) reuses `http.client`, `ipaddress`, `json` and `urllib.parse`. Its project logic bounds metadata, preserves exact request bytes, checks loopback/response identity and distinguishes explicit refusal from unknown completion. A fresh connection per operation is appropriate to the qualified local scope; connection pooling requires a measured request-rate need. Do not introduce requests/HTTPX just to gain a pool or automatic retries. The HTTP response decoder already handles chunk framing; the adapter does not hand-decode it.

The experiment scripts capture package versions, but do not constitute a pinned install manifest for every imported dependency. A metadata-only check of the recorded bare Python executable currently finds PyArrow 21.0.0 and no PyIceberg/blake3 distributions, while the acceptance receipt records PyArrow 23.0.0, PyIceberg 0.12.0 and blake3 1.0.11. This does not invalidate the retained run; it shows that an executable path alone does not reconstruct its package environment or any launch-time path configuration. Before another controlled experiment, freeze the actual environment with its receipt or a maintained lock workflow. This is reproducibility work, not a claim that an unpinned package currently causes poor performance.

## Build and development tools

| Tool or package family | Current use and decision |
| --- | --- |
| Rust/Cargo, rustfmt, Clippy | [rust-toolchain.toml](../rust-toolchain.toml) selects Rust 1.94.0 and the maintained formatter/linter. [scripts/check](../scripts/check) runs formatting, locked all-target/all-feature Clippy and tests. **Keep.** The audit inspected configuration rather than launching this gate. Compiler jobs are separately limited in [.cargo/config.toml](../.cargo/config.toml); they are not engine compute threads. |
| Cargo build profiles | [Cargo.toml](../Cargo.toml) disables development debug information and incremental compilation, and retains limited release debug information. **Keep the disk-conscious defaults.** Incremental compilation trades additional target storage for rebuild work; LTO/codegen tuning trades build time and memory for possible runtime improvement. Neither is a free performance switch. Measure clean/incremental build cost and peak additional disk separately from dataset acceptance before changing them. [Cargo profile tradeoffs](https://doc.rust-lang.org/cargo/reference/profiles.html). |
| `cc` 1.6.0, `pkg-config` 0.3.34, `vcpkg` 0.2.15 and native `-sys` build scripts | Present in Cargo.lock through maintained native bindings; the inspected mimalloc, Zstandard and SQLite manifests use upstream build facilities. **Keep.** Do not replace these with project-owned compiler/linker scripts or force system libraries without a concrete packaging constraint. Lockfile presence is not proof that every target executes every helper; no host compiler or linker version was measured here. |
| `proc-macro2` 1.0.107, `quote` 1.0.47, `syn` 2.0.119 / 3.0.6 | Transitive Rust macro infrastructure supports the maintained derives/async interfaces. **Keep.** Multiple parser versions affect build work, not per-row runtime parsing. No custom macro expansion or incompatible dependency override is justified. |
| Python standard library, shell, Git and OS measurement tools | Adapter tests use `unittest`; experiment scripts use maintained `subprocess`, JSON and path facilities, Git for source identity, and `/usr/bin/time` alongside psutil for measurements. **Keep.** They are control/qualification tools, not extra production compute engines. Existing subprocess capture/recovery serves reproducibility; do not replace it with a scheduler or monitoring framework. Host-tool versions belong in the next experiment's environment receipt when material. |

## Custom code that remains justified

| Project-owned behavior | Why a package feature is not a drop-in replacement |
| --- | --- |
| Dataset-scoped guarded lifecycle selection, stable operation identities and response reconciliation | Iceberg commits one native table. DocSpec selects related snapshots, schemas, evidence, disclosure and recoverable phases together through its lifecycle table. Native blind retry cannot re-evaluate those domain decisions. `commit.retry.num-retries=0` is deliberate. |
| Selected-schema and occurrence adapters | Pinned native transformation needs the transient `_file` field to enforce field-ID mapping. Physical offsets, stored origins, complete-file membership and absence interpretation are DocSpec guarantees. The compatible external Iceberg DataFusion provider does not supply all of these plus resource admission/statistics. |
| Owned memory permits and bounded serialization sinks | DataFusion's memory pool covers participating operators, not every external decoder, retained manifest, output chunk or blocking task. Standard serializer/writer calls do not by themselves enforce the application's admission lifetime. Keep small wrappers around maintained implementations. |
| Admission copy/hash and flat-output hashing sink | A required transfer/write can establish its byte identity in the same pass. A plain file copy followed by a package hash convenience function would read again. Native writer digest integration remains deferred until its extra pass is measured. |
| Protection inventory, deletion reservation/outcomes, physical compaction and restore selection | Native snapshot expiration is not authority to remove cross-dataset evidence, active readers, completed phases or supported restore points. The current implementation reuses native transactions/writers and SQLite `VACUUM INTO`; it does not need another backup engine merely to avoid domain checks. |
| Independent qualification reader and exact comparator | A maintained DuckDB/PyIceberg path provides independent reconstruction. Reusing production output as its own expected answer would erase this check. Duplicate scans across this explicit boundary can be necessary; they should remain labeled and measured. |
| Structured library error/refusal codes | Anyhow is appropriate at the CLI boundary, while callers need stable refusal codes and retained causes for recovery. Current code is a small adapter, not a replacement error ecosystem. |

## What to keep deferred

Keep one native provider integration and the compatible pinned family. The existing research inspected DataFusion 55.2 with Arrow/Parquet 59.2; this repository uses Arrow/Parquet 58.4 with Iceberg 0.10.1. A later version is not a compatible one-line improvement. Upgrades need exact-value, field-ID/default, occurrence, filtered-count and null-safe-join regressions. Do not assume a later hash join spills simply because the runtime has a spill directory.

The pinned Iceberg reader already exposes `with_metadata_size_hint`, `with_range_coalesce_bytes` and `with_range_fetch_concurrency`. Its defaults are 512 KiB footer prefetch, 1 MiB coalescing gap and 10 range fetches. These affect different costs: footer round trips, overfetch between ranges and simultaneous buffers. **One active file reader does not imply one range request.** Current local data cannot establish the best values for R2. Use the injected storage adapter and actual provider request/byte evidence when that scope is qualified. [Pinned reader configuration](https://github.com/apache/iceberg-rust/blob/v0.10.1/crates/iceberg/src/arrow/reader/mod.rs).

Cargo's features are additive across consumers. DataFusion enables Arrow defaults and Parquet defaults; Iceberg enables Parquet encryption; several libraries enable Futures defaults. Root-manifest cleanup alone may change nothing. Removing SQL function families can also change the supported authored-query surface. Keep those until a concrete build or capability decision justifies narrowing. [Cargo feature unification](https://doc.rust-lang.org/cargo/reference/features.html#feature-unification).

Keep the current SQL catalog and restore approach. SQLite documents `VACUUM INTO` as a consistent compact copy and the backup API as an incremental alternative with lower CPU work. The current restore path explicitly syncs and checks the new catalog. No measured catalog-copy bottleneck justifies a second database driver or a custom backup implementation. [SQLite backup tradeoff](https://www.sqlite.org/lang_vacuum.html#vacuum_with_an_into_clause).

Future implementation should take one decision-table item at a time, preserve independent qualification, and measure the complete path only when making a complete-path claim. The practical finding is underused maintained integration points, not a need to replace the package stack.

## Appendix: complete resolved direct-dependency feature sets

Generated from the offline locked metadata graph during this audit. `default` appears where Cargo reports that feature; an empty set means no enabled crate features. Development dependencies appear in the same graph. Cargo may unify additional features for a different target or command.

| Role | Package | Requested features | Resolved version and complete features |
| --- | --- | --- | --- |
| production | `anyhow` | defaults | 1.0.104; `default`, `std` |
| production | `arrow` | defaults + ipc, json | 58.4.0; `arrow-csv`, `arrow-ipc`, `arrow-json`, `chrono-tz`, `csv`, `default`, `ipc`, `json`, `prettyprint` |
| production | `async-trait` | defaults | 0.1.92; none |
| production | `axum` | defaults | 0.8.9; `default`, `form`, `http1`, `json`, `matched-path`, `original-uri`, `query`, `tokio`, `tower-log`, `tracing` |
| production | `blake3` | defaults | 1.8.7; `default`, `std` |
| production | `clap` | defaults + derive | 4.6.7; `color`, `default`, `derive`, `error-context`, `help`, `std`, `suggestions`, `usage` |
| production | `datafusion` | defaults | 53.1.0; `bzip2`, `compression`, `crypto_expressions`, `datafusion-datasource-parquet`, `datafusion-functions-nested`, `datafusion-sql`, `datetime_expressions`, `default`, `encoding_expressions`, `flate2`, `liblzma`, `nested_expressions`, `parquet`, `recursive_protection`, `regex_expressions`, `sql`, `sqlparser`, `string_expressions`, `unicode_expressions`, `zstd` |
| production | `fs2` | defaults | 0.4.3; none |
| production | `futures` | defaults | 0.3.34; `alloc`, `async-await`, `default`, `executor`, `futures-executor`, `std` |
| production | `iceberg` | defaults | 0.10.1; `default` |
| production | `iceberg-catalog-sql` | defaults | 0.10.1; none |
| production | `mimalloc` | defaults | 0.1.52; `default` |
| production | `parquet` | defaults + arrow | 58.4.0; `arrow`, `arrow-array`, `arrow-buffer`, `arrow-data`, `arrow-ipc`, `arrow-schema`, `arrow-select`, `async`, `base64`, `brotli`, `default`, `encryption`, `flate2`, `flate2-zlib-rs`, `futures`, `lz4`, `lz4_flex`, `object_store`, `simdutf8`, `snap`, `tokio`, `zstd` |
| production | `serde` | defaults + derive | 1.0.229; `alloc`, `default`, `derive`, `rc`, `serde_derive`, `std` |
| production | `serde_json` | defaults | 1.0.151; `alloc`, `default`, `raw_value`, `std` |
| production | `sqlx` | defaults off + runtime-tokio, sqlite, any | 0.8.6; `_rt-tokio`, `_sqlite`, `any`, `runtime-tokio`, `sqlite`, `sqlx-sqlite` |
| production | `tokio` | defaults + macros, rt-multi-thread, sync, net, signal | 1.53.2; `bytes`, `default`, `fs`, `io-util`, `libc`, `macros`, `mio`, `net`, `rt`, `rt-multi-thread`, `signal`, `signal-hook-registry`, `socket2`, `sync`, `time`, `tokio-macros`, `windows-sys` |
| production | `tracing` | defaults | 0.1.44; `attributes`, `default`, `log`, `std`, `tracing-attributes` |
| production | `tracing-subscriber` | defaults + env-filter | 0.3.23; `alloc`, `ansi`, `default`, `env-filter`, `fmt`, `matchers`, `nu-ansi-term`, `once_cell`, `registry`, `sharded-slab`, `smallvec`, `std`, `thread_local`, `tracing`, `tracing-log` |
| production | `uuid` | defaults + v4, serde | 1.27.0; `default`, `js`, `rng`, `serde`, `std`, `v4`, `v7` |
| development | `tempfile` | defaults | 3.27.0; `default`, `getrandom` |
| development | `tower` | defaults + util | 0.5.3; `futures-core`, `futures-util`, `log`, `make`, `pin-project-lite`, `retry`, `sync_wrapper`, `timeout`, `tokio`, `tracing`, `util` |
