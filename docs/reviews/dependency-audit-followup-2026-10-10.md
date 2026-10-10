# Dependency audit follow-up

The existing package audit covers the current dependencies. Keep its priorities:
reuse captured metadata, remove redundant relational work, then qualify native
writer layout. This follow-up found two additional hash-only JSON buffers that
can use the existing helper. It found no new reason to enable package features,
replace the dependency family, or introduce another cache or scheduler.

This is a read-only source audit of committed
`b055102a7b992312b65ec9cfc50a0addefbd9577`. Uncommitted change-history work was
excluded. No builds, tests, formatting, benchmarks, corpus reads, or data
downloads were performed. Only this document was written. Findings describe
structural effects; they establish no new performance measurement.

## Coverage confirmation

The SHA-256 identities of `Cargo.toml`, `Cargo.lock`, and
`adapters/dagster/pyproject.toml` match the identities in the
[existing audit](../dependency-efficiency-audit-2026-10-10.md#scope-and-evidence).
Its pinned Rust versions and effective feature inventory therefore remain
applicable. This check read the committed manifests and lockfile; it did not
rerun dependency resolution.

Every direct production and development Rust package has a disposition in that
audit. Syntax-tree inspection of every tracked Python file confirms that its
adapter/build and experiment package inventory is also complete: Dagster,
Pydantic, setuptools, DuckDB, PyArrow, PyIceberg, psutil, Python blake3, and NumPy.
The other imports are Python standard-library modules or local project modules.
There is no direct Rust build-dependency section. The existing transitive and
re-export inventory covers the reviewed calls; the current comparison code
continues to use the already documented DataFusion aggregate/window APIs.

The existing qualification-environment caveat remains: the experiment scripts
and retained version receipts do not together constitute a complete install
lock. Freeze the actual environment before a new controlled experiment. That
does not justify rerunning an accepted corpus or changing packages now.

## Additional call-site omissions

**Act as a small consistency cleanup:** use the existing
[`storage::json_digest`](../../src/storage.rs#L56) in these pure hash-only sites:

| Committed call site | Maintained operation and effect | Qualification boundary |
| --- | --- | --- |
| [`fingerprint`](../../src/engine.rs#L2048) | The request's material JSON is serialized into a temporary `Vec` solely for BLAKE3. Pass the unchanged `(PROCESSOR, material)` value to `json_digest`. | Preserve the exact material selection and compact serialization. The existing `Value` tree remains necessary to preserve current identity behavior; this recommendation removes only the byte vector. |
| [`schema_identity`](../../src/storage/inspection.rs#L50) | The normalized physical-schema JSON is serialized into a temporary `Vec` solely for BLAKE3. Hash that same normalized value through `json_digest`. | Preserve removal of `doc`, `initial-default`, and `write-default`, object ordering, and the same error conversion. Do not replace the normalization logic. |

Neither vector is persisted, returned, or used to enforce a byte allowance.
For serialized metadata of size J, the change removes one O(J) temporary byte
allocation at each call, while serialization and hashing remain O(J). It removes
no payload scan, disk write, or network request. Its practical value is shared
implementation and lower transient allocation; no elapsed-time or process-memory
improvement has been measured.

The pinned implementation confirms equivalence: `serde_json` 1.0.151
`src/ser.rs:2213` implements `to_vec` by writing through `to_writer` into a Vec;
the latter uses the ordinary compact serializer at `src/ser.rs:2177`. BLAKE3
1.8.7's `src/lib.rs:1627` implements `Write` by calling `update`. The helper
already composes these maintained APIs. See the pinned
[Serde JSON stream writer](https://docs.rs/serde_json/1.0.151/serde_json/fn.to_writer.html)
and [BLAKE3 writer](https://docs.rs/blake3/1.8.7/blake3/struct.Hasher.html#impl-Write-for-Hasher)
documentation. Check existing frozen digest expectations when implementing;
no new serialization abstraction is needed.

## Existing priorities confirmed

**Act next on shared metadata use.** The origin-check adapter still
[deep-clones admitted metadata](../../src/engine/backfill/origins/checks.rs#L249)
even though Iceberg 0.10.1
[`TableBuilder::metadata`](https://github.com/apache/iceberg-rust/blob/v0.10.1/crates/iceberg/src/table.rs#L74)
accepts `Into<TableMetadataRef>` and `TableMetadataRef` is an `Arc`. Passing the
existing Arc avoids rebuilding the metadata object graph. This confirms an
existing finding, not a newly discovered issue. Keep the metadata's admission
lifetime with its users.

The larger repeated-work opportunity also remains:
[`NativePartition::execute`](../../src/query.rs#L317) opens ordinary occurrence
readers through [`snapshot_files`](../../src/storage.rs#L519), which traverses
all snapshots and unique manifests. Reuse an admitted captured inventory within
the exact immutable operation before considering a cache. With K repeated
executions and H retained snapshots/M unique manifests, this can remove K-fold
planning work over H and M. Retention/admission still needs its complete closure;
ordinary scans must not silently weaken those rules. Upstream cache reuse alone
does not address the explicit manifest loads in this path.

**Keep the separate safety boundaries.** The
[removal hash loop](../../src/storage/removal.rs#L152) also checks growth beyond
the retained byte count and verifies file identity before unlinking. The ordinary
`update_reader` helper does not replace that full procedure. Preserve the
bounded schema-identity buffer in
[`engine/schema.rs`](../../src/engine/schema.rs#L224), which also enforces a size
allowance, and buffers that are subsequently persisted. A broader rewrite of
these paths needs its own concrete benefit and equivalence check.

**Defer tuning and upgrades.** Native row-group size, file/range concurrency,
remote request coalescing, parallel hashing, blanket optional-feature trimming,
and dependency upgrades still require the controlled evidence already specified
by the main audit. No new package finding supersedes that ordering. Measure peak
additional disk and retained bytes separately from execution time; the small
hash-only cleanup above does not change disk amplification.

## Build storage follow-up

The root's subsequent [lifecycle gate](optional-history-full-check-2026-10-10.log)
failed during linking with `No space left on device`. A partial inventory found
5,855,550,000 logical bytes of test/build executables under `target/debug/deps`;
this is build storage, not dataset amplification. The root continued one target
at a time and reclaimed only each successfully executed test binary.

The independent auditor recommends Cargo's standard integration-test grouping
as the lasting fix: ordinary suites can be modules of fewer `[[test]]` targets,
reducing repeated dependency linking. Existing CLI recovery tests still spawn
the real CLI through `CARGO_BIN_EXE_docspec`. Keep `worker_initialization`
separate because it intentionally retains a host descriptor until process exit.
Keep `restore` separate initially: its self-spawned `--exact` child selector
would need its new module-qualified name. Grouping also requires adjusting the
shared `crate::scalar` import. Measure the resulting artifact size and complete
gate time; editing one module will relink its larger group.
[Cargo target guidance](https://raw.githubusercontent.com/rust-lang/cargo/rust-1.94.0/src/doc/src/reference/cargo-targets.md)
explicitly describes this organization.

The empty CLI unit-test harness can separately use `test = false`; integration
tests still build the CLI. Keep the current `debug = 0` and `incremental = false`
settings, inherited by the test profile. One build job reduces concurrent linker
scratch, not retained executable totals. Avoid new linking flags or a permanent
custom test runner before trying native target organization. These are researched
recommendations; no Cargo or test-layout change was made by this audit.
[Cargo profiles](https://raw.githubusercontent.com/rust-lang/cargo/rust-1.94.0/src/doc/src/reference/profiles.md).
