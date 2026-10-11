# Remote publication: smallest useful next slice

Static review, 2026-10-10. No implementation, builds, tests, credentials, or provider calls occurred in this review.

## Decision and findings

**Keep Basin as the eventual sole dataset-selection authority. Build the shared FileIO boundary next, then extend the existing export phases.** An R2 copy of a local export is useful distribution work, but it cannot complete the planned Basin-selected native release. Do not mirror the local channel into a second remote lifecycle: its source eligibility check and remote selection would occur under different authorities.

| Choice | User value | Cost and limit |
| --- | --- | --- |
| Extend local `Engine::export` with R2 transfer/readback | A consumer can read an immutable authorized Parquet file outside the producer workspace. | Smallest actual upload slice; local lifecycle still selects the channel. Basin selection, canonical remote native data/evidence, and remote recovery remain unfinished. |
| **Use the existing engine with Basin and shared remote object I/O** | The selected native release and its derived download can be reproduced remotely through one lifecycle. | More callers change, but the work removes filesystem assumptions instead of adding a temporary publication authority. Recommended route toward the accepted plan. |

The optional catalog adapter only supplies REST catalog/S3 table I/O (`src/storage/remote.rs:3`, `:34`). `Engine::with_catalog` injects a catalog, but native metadata writes, object identity, and pinned reads still use local files (`src/engine.rs:213`; `src/storage/tables.rs:81`; `src/storage.rs:623`, `:716`). Merely substituting the catalog cannot establish the second outcome.

The existing export service already provides the lifecycle to extend: protected exact-source admission, retained materialization/validation, original-result retry, source-order and authority checks, and one result/channel commit (`src/engine/export.rs:141`, `:235`, `:305`, `:327`). Preserve it. Native releases remain canonical; flat files remain derived distributions (`docs/requirements/02-incremental-publication.md:13`, `:23`).

## Smallest durable patch boundary

First make the existing object helpers accept the maintained `iceberg::io::FileIO`, injected at startup with an owned object-root URI. Keep the local scratch directory and host ownership separate. Use concrete FileIO parameters and shared helpers, not another storage trait, lifecycle, uploader database, or adapter registry.

| Existing boundary | Required change for a Basin-selected source release |
| --- | --- |
| Composition: `src/engine.rs:228`; `src/storage/remote.rs:34` | Compose one catalog authority and matching object I/O from the same selected provider configuration. Keep secrets out of retained operation records and debug output. |
| Object identity/admission: `src/storage.rs:156`, `:420`, `:455`, `:468`; `src/engine.rs:826`, `:1781`, `:1829` | Bounded stat/read/write/copy through FileIO; hash while admitting/transferring; reuse known immutable identities. Replace durable `PathBuf` construction with validated storage locations. Local source input and scratch may remain local. |
| Native creation and reads: `src/storage/tables.rs:37`, `:81`; `src/storage.rs:623`, `:716`; `src/storage/inspection.rs:120` | Write candidate metadata through the injected I/O, retain remote identities, open exact pins with that I/O, and perform bounded footer/sample checks through maintained readers. Pass the same I/O through existing read/write callers rather than adding remote variants. |
| Release metadata: `src/engine.rs:1604`; `src/engine/inputs.rs:399`; `src/engine/read.rs:152` | Write/read bounded manifests and catalog-free pins remotely. Ordinary reads reuse admission; do not add payload verification to every open. |
| Source execution: `src/engine/write.rs:128`, `:251`; `src/budget.rs:100` | Remote retained Parquet inputs also require a maintained DataFusion object-store registration, or the existing shared reader feeding its table provider. Iceberg FileIO injection alone does not configure `register_parquet`. Choose one maintained path. |
| Protection: `src/engine/retention/inventory.rs:776`, `:1240`, `:1632`, `:1799`; `src/engine/retention/owners.rs:741`, `:994` | Admit only configured remote scheme/bucket/owned-prefix references and record the same exact roots. Preserve strict local path checks. Remote URI support must not make remote objects eligible for the local unlink/restore implementation. |

Prove this boundary with a small source dataset using the same admission → native output → validation → release selection → fresh reader path. This prerequisite is independently useful, but its completion is not remote publication completion. Broad backfill/refresh/compaction paths must receive the shared I/O before those operations are advertised remotely; remote destructive maintenance and restore remain explicitly unavailable until qualified.

## Publication extension, after that boundary

Extend `src/export.rs` and `src/engine/export.rs`; retain the current authorized primitive-column/all-active-row scope. The writer already removes private metadata and hashes while writing (`src/engine/export.rs:646`, `:806`). Transfer only that materialized file, never the native source file or its full metadata inventory.

1. Bind the exact source, policy, operation ID, destination identity, expected channel, and original authority epoch in the durable export request. Protect the immutable destination before writing; retain transfer and readback completions as additional records in the same lifecycle. Include them in the existing protection inventory and creation-identity lookup.
2. Stream the local materialization once to the destination under `Budget`, with bounded chunks, bounded concurrent work, and backpressure. Reuse its existing digest and creation validation. Do not add a pre-upload full hash pass, full-file buffer, bucket scan, or extra local copy.
3. Independently stream the newly transferred remote bytes once and compare exact length and BLAKE3 identity. Matching bytes inherit the already retained format checks; another full decode/reconstruction is qualification work, not routine publication. Retain this readback result before selection. An ETag or file size alone does not prove identity.
4. Recheck current eligibility, the original authority epoch, expected channel, and source nonregression; commit `export-result` and `export-channel` once through the same Basin lifecycle. Read back serialized selection metadata. A lost response resolves the original operation; it does not upload again or advance a channel that subsequently changed.

The pinned FileIO API supports range reads and incremental writes, but exposes no conditional-create option (`iceberg-0.10.1/src/io/file_io.rs:251`, `:316`, `:367`; `iceberg-storage-opendal-0.10.1/src/lib.rs:526`). Use exclusive attempt identities and durable ownership; never reopen a confirmed object for writing. Reconcile an unknown completed write by exact readback before any retry. **Do not treat `exists` followed by an overwriting writer as immutability protection.** If competing or delayed writers cannot be excluded at that boundary, qualify the maintained provider's conditional write capability before publishing; do not invent a signer or claim concurrent-host safety from a local lock. Measure the maintained writer's internal buffering as well as application chunks.

## Necessary checks and completion claims

- Extend `tests/exports.rs` and register any new suite in `tests/integration.rs`: exact remote values/types/nulls/duplicates/empty output; private-column and footer canaries absent; unchanged source/output identities reused.
- Interrupt after transfer, remote readback, and catalog selection; retry the same ID and verify zero repeated completed uploads/readbacks. Exercise changed-body/same-ID refusal, unknown write response, damaged remote bytes, stale epoch, channel conflict, and regressing source. Keep the prior channel intact on every preselection failure.
- Verify transfer/readback/result roots survive parent failure and remain visible to the shared retention inventory. Check remote objects cannot reach local removal or local restore. Keep expiration disabled.
- Qualify a fresh independent reader against the actual R2 consumer access path, and Basin expected-snapshot conflicts/lost-response recovery at the actual catalog boundary. Record bytes, requests, retry work, peak memory and scratch. Local fault fixtures do not establish provider behavior.
- Run the repository gate and independent review when implemented. No performance improvement is claimed here; the proposed structural improvement is one storage path and reuse of the existing lifecycle and validation.

These checks derive from `docs/PLAN.md:44`, `:69`, `:89`, `:96`, `:124`; R2 transfer/authority/readback tasks (`docs/requirements/02-incremental-publication.md:35`); and R6 shared roots and exact retirement (`docs/requirements/06-maintenance-retention.md:23`). A native table plus a flat file is still insufficient if required evidence or referenced metadata remains available only inside the producer workspace.

**Product decision:** none is needed to implement or qualify an isolated fixture against the already selected R2/Basin provider. Before a real public release, the caller must supply its exact disclosure policy and intended reader access path; anonymous hosting and availability windows require an explicit product choice. Use existing authenticated access for qualification without changing bucket policy. R2 requires those choices before format freeze, not before fixture work (`docs/requirements/02-incremental-publication.md:46`); R6 defines no automatic deletion window (`docs/requirements/06-maintenance-retention.md:57`).

**Verdict: RECONSIDER an upload-only completion claim; APPROVE the shared-I/O route.** It honors one selection authority and pays down filesystem coupling. The falsifier is a purported remote pin that requires local paths, another mutable selection, or repeated completed transfers to recover. Provider immutability, complete native remote operation coverage, cross-host fencing, remote maintenance/restore, and performance remain unqualified until their own evidence exists.
