# Data compaction prerequisites

Static design analysis of the local implementation at `ecb31f1`, October 9,
2026. This note records proposed work. It does not establish implemented
compaction, measured savings, or R6 acceptance. No builds or tests were run for
this analysis.

## Decision

Implement durable occurrence identity and separate representation selection
before exposing source data compaction. Reusing the native writer is straightforward;
preserving backfill reuse, evidence, logical versions and retention promises is the
substantial work.

[R6](../requirements/06-maintenance-retention.md#physical-maintenance) requires
equivalent representations to preserve values, occurrence identities, field
meaning and generation history without advancing the logical dataset version.
Compaction does not independently authorize deletion. The current
[occurrence checkpoint](../STATUS.md#source-occurrence-checkpoint) explicitly
leaves identity across compaction unfinished.

## Blocking seams

| Current behavior | Consequence | Required change |
| --- | --- | --- |
| `storage/occurrences.rs:19–27,149–156,382–406` generates identity from the current file path and physical row position. | Rewriting a file changes its occurrence addresses, even when values are identical. | Persist origin identity independently of current physical location. Preserve duplicates as distinct occurrences. |
| `engine/backfill.rs:176–187` joins source and computed groups on those addresses; `backfill/verification.rs:410–467` checks coverage against current physical files. | A source rewrite breaks combined reads or coverage verification. | Move joins and coverage to the same stable identity mechanism. |
| `model.rs:787–795` binds a computed group to source metadata; `backfill/verification.rs:299–315` requires exact metadata equality; `backfill.rs:1011–1016` keys file reuse by `ObjectRef`. | Physical replacement appears to change the computation input and invalidates completed work. | Separate logical input identity from the exact physical snapshot consumed; retain both meaning and execution provenance. |
| `model.rs:73–134` embeds physical inventories in dataset/releases; `engine/read.rs:74–89` accepts exact retained release equality; `history.rs:95–101` rejects another manifest for the same version. | Replacing `result/{version}` or calling normal logical selection would break existing authority and history checks. | Select an immutable equivalent representation separately from logical version selection. |
| `engine/incremental.rs:339–347` discovers changes from physical file membership. | Compaction triggers unnecessary downstream recomputation. | Resolve equal logical group state before opening payloads or discovering changed keys. |
| `retention/inventory.rs:784–820` expands computed base manifests, parent manifests and change endpoints into old physical closures. | Adding compacted pins can preserve the old files indefinitely; weakening traversal could delete required inputs. | Distinguish historical interpretation records from physical availability, while preserving exact reader, release, phase and input dependencies. |

Code references above and below are relative to `src/` and describe the reviewed
revision.

## Maintained components to reuse

`storage::write_batches` already streams bounded Arrow batches through Iceberg's
`ParquetWriterBuilder`, `RollingFileWriterBuilder` and `UnpartitionedWriter`, with
Zstd (`storage.rs:170–239`). `replacement_metadata` preserves native schemas and
allocated field IDs in an independent candidate; `register_candidate` durably
writes and registers that metadata (`storage/tables.rs:37–104`). Reuse these
paths and share unaffected `DataFile`s and known `ObjectRef`s.

The locally cached, pinned `iceberg 0.10.1` transaction API exposes append,
schema/properties/location/statistics changes and expiration, but no data-file
rewrite action (`iceberg-0.10.1/src/transaction/mod.rs:134–170`). Its V3 append
tests allocate row ranges, but the reader pipeline excludes metadata fields from
Parquet projection and synthesizes only `_file`
(`iceberg-0.10.1/src/arrow/reader/pipeline.rs:214–248`). Static inspection found no
complete stable-row-ID read/rewrite path to adopt. Switching formats alone is
insufficient; qualify a maintained path before relying on one.

Pinned `datafusion 53.1.0` provides streaming execution and relational comparison.
The existing duplicate-aware comparison at `engine/refresh.rs:784–799` is useful,
but its type coverage must be qualified before treating it as R6's general
nested/null/decimal/time equivalence check.

## Smallest coherent implementation sequence

1. **Persist occurrence identity.** Carry internal origin fields unchanged through
   compaction and the kept-row branch of source updates
   (`engine/write.rs:426–507`); assign fresh identities to new occurrences. Existing
   birth-file/birth-row tuples can seed retained identities without guessing from
   business keys or values. Update `OccurrenceScan`, `query::occurrences`, scalar
   coverage and contributor relations together. Keep physical locations as reader
   metadata. Prefer native columns over per-row catalog records or chained
   relocation tables.

2. **Separate logical state from representation.** Keep `TablePin` as an exact
   physical pin. Define logical group-state identity and an immutable
   representation record binding that state to physical pins, equivalence evidence
   and its creation operation. Select it through the existing dataset lifecycle
   table. Resolve it once per request; serialized resolved pins remain independently
   readable. Version, definitions, coverage, processing history and succession do
   not change. Update release resolution, read admission, input bindings and
   retention traversal consistently. Preserve old exact pins while their promises
   stand.

3. **Preserve completed-work reuse.** Base material reuse on logical source
   identities and relevant field dependencies. Preserve the consumed snapshot in
   completion provenance/protection. Equal logical state bypasses changed-key
   discovery. Physical compaction must not rerun a completed backfill or unchanged
   downstream refresh.

4. **Rewrite bounded file groups.** Capture the representation and native schema
   once. Concatenate only chosen files through the existing writer, reuse other
   descriptors, and assemble a replacement candidate. Separate output-file target
   size from `max_batch_bytes`, currently coupled at `storage.rs:189–192`; larger
   files must not enlarge decoded-batch admission. The current inventory refuses
   native delete files (`storage.rs:425–429`), so support for existing materialized,
   unpartitioned layouts alone remains partial R6-T02.

5. **Retain completion and exact equivalence.** Reserve inputs and output custody
   before production, then retain completed bounded rewrites for resume. Reuse known
   object identities; hash new durable files once through the existing pin boundary
   (`storage.rs:518–532`). For concatenation, retain input/output file order and
   compare aligned typed streams at the equivalence boundary. Check identities,
   values, types, null/default/supplied meaning, duplicates and relevant order.
   Counts alone are insufficient. An independent source reread here serves the
   required correctness check; do not repeat it at selection or ordinary reads.

6. **Select and reclaim through existing authority.** Recheck logical state and
   expected representation under the dataset lock and guarded lifecycle commit.
   Preserve compatible sibling field groups; retain completed work if selection
   becomes stale. Use ordinary admitted work for rewriting so existing readers and
   writers continue. Add in-flight, completed and selected representations to the
   common protection inventory. Reclaim eligible objects only through existing
   removal preview/execution; exclusive maintenance already guards that operation
   (`engine/worker.rs:111–155`, `retention/removal.rs:252–258`).

## Evidence required before claiming support

- Compact duplicate/null values, schema renames/defaults, scalar backfills and
  downstream join/summary evidence. Preserve logical versions, occurrence IDs,
  exact values, evidence and succession.
- Show that completed backfills and unchanged downstream refreshes avoid
  recomputation after compaction.
- Exercise compaction during sibling backfill and an open old-reader stream;
  preserve both exact representations and reject stale incompatible selection.
- Interrupt after saved rewrite, equivalence and selection boundaries; resume
  without repeating acknowledged work.
- Measure actual file opens and query latency against rewrite/verification I/O,
  memory and peak coexistence storage. Report reclaimed bytes separately: retained
  history may legitimately prevent reclamation.

Compacting only computed or evidence tables could later provide a bounded slice
because their source identifiers are already stored values. It still requires
representation selection and does not establish source compaction or complete R6.
