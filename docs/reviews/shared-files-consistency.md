# Shared native files: consistency review

**Final verdict: APPROVE for the native file-sharing primitive.** The implementation
now inventories statistics references and refuses conflicting inherited object
identities. The initial findings below are retained as review evidence; the closure
section records the fixes. This checkpoint does not implement incremental
application selection or retention.

This is a static semi-formal review of the uncommitted diff against `8f6fa6a` on
2026-10-07. It covers `src/storage.rs`, its changed `src/engine.rs` callers, the
native unit probe and the relevant maintained Iceberg implementations. The reviewer
did not run tests or benchmarks and did not modify implementation. Earlier Rust
and semi-formal-review skill guidance remains applicable.

## Initial findings

### F1 — Statistics references escape the claimed native inventory

- **Severity:** WARNING
- **Category:** correctness
- **Location:** `src/storage.rs:279-328` in the initially reviewed diff.
- **Description:** `snapshot_files` inventories the metadata file, snapshot
  manifest lists, manifests and live data/delete files, but does not inspect
  native statistics or partition-statistics references.
- **Evidence chain:** The only path discovery loop is over `snapshots()` and their
  manifests (`286-327`). The pinned maintained `TableMetadata` separately exposes
  `statistics_iter()` and `partition_statistics_iter()`
  (`iceberg-0.10.1/src/spec/table_metadata.rs:386-397`). These are native referenced
  objects outside the manifest graph. Consequently, `pin` does not enforce their
  ownership (`355-403`), and `check_inventory` cannot detect their omission
  (`406-424`). A table whose metadata references an external statistics file can
  be pinned without an inherited owner for that file.
- **Suggestion:** Add those paths to the same inventory, or explicitly refuse
  tables containing those unqualified reference forms. A refusal is sufficient
  for the present materialized-file primitive. Do not present this limited
  inventory as a complete restore descriptor; prior metadata-log closure is
  expressly excluded by the current comment (`284-285`).
- **Coverage:** The new probe creates ordinary append snapshots without native
  statistics sidecars. It cannot establish coverage of this branch.

### F2 — Conflicting inherited objects silently choose the last owner

- **Severity:** WARNING
- **Category:** correctness
- **Location:** `src/storage.rs:375-378` in the initially reviewed diff.
- **Description:** Collecting the caller's shared references into a `BTreeMap`
  silently replaces an earlier reference for the same path. Conflicting owners
  therefore become an argument-order-dependent ownership decision.
- **Evidence chain:** The map key is the physical path, while the value contains
  owner, digest and size. When pinning a shared object, the chosen map value is
  verified and cloned unchanged (`389-391`). If two entries have the same path,
  digest and size but different owners, either owner passes byte verification;
  whichever appears last is retained in the new pin. If digests conflict, argument
  order likewise decides which declaration is evaluated.
- **Concrete failure sequence:** Clone one source `ObjectRef`, change only its
  owner, and provide both entries to `pin`. Reverse their order. The candidate can
  receive different inherited ownership with identical native files and bytes.
- **Suggestion:** Collapse identical duplicate declarations but refuse conflicting
  declarations for a physical path. Add a negative test for conflicting owners.
  This requires no new ownership service; the actual durable owner/root protocol
  remains outside this storage checkpoint.
- **Coverage:** The probe passes one well-formed source inventory and checks the
  retained owner. It does not test contradictory inherited references.

## Patch summary

The patch replaces directory traversal with native snapshot/manifest traversal.
New pins include files outside the candidate table's directory only when the
caller supplies a byte-verified inherited reference. The current run path calls
the new asynchronous pin helper after native append (`src/engine.rs:552-565`).
Independent table verification now checks that the pin includes every discovered
native dependency before decoding rows (`967-983`).

This is a useful prerequisite for incremental updates: the probe creates a new
candidate table using an unchanged original Parquet file and a new replacement
file. It is not an incremental application update, lifecycle selection, ownership
transfer, cross-dataset protection reservation or retention implementation.

## Function trace

| Function / method | Location in initial diff | Input | Output | Verified behavior |
| --- | --- | --- | --- | --- |
| `snapshot_files` | `src/storage.rs:279` | Native table, budget | Referenced paths and current live `DataFile`s | Traverses all snapshots retained by exact metadata, loads maintained manifests, filters live entries, refuses current delete files and duplicate current data paths. |
| `check_inventory_size` | `src/storage.rs:336` | Path set, counted metadata bytes | Capacity result | Bounds metadata reads and estimated per-object admission; does not claim a complete RSS ceiling. |
| `pin` | `src/storage.rs:355` | Owner, table, row count, budget, shared refs | TablePin | Checks native row count, preserves a supplied inherited object, or hashes a canonical path within the candidate's own root. F1/F2 apply. |
| `check_inventory` | `src/storage.rs:406` | Pin, opened table, budget | Completeness result | Requires the pinned metadata reference and every discovered native path to appear in the inventory. Extra retained objects are allowed. |
| `open_pin` | `src/storage.rs:431` | TablePin | Catalog-free native table | Verifies metadata identity and exact current snapshot/schema binding. Behavior unchanged. |
| `verify_table` | `src/engine.rs:967` | TablePin, budget | Verification result | Hashes listed objects, opens metadata, checks discovered path coverage, decodes bounded native batches and compares row count. |
| `run_internal` processing branch | `src/engine.rs:552-565` | Admitted input and SQL stream | Completed processing phase | Writes/appends fresh native files, awaits pinning, then commits phase completion. Uses no shared references yet. |
| Native `Table::manifest_list_reader` | `iceberg-0.10.1/src/table.rs:305` | Snapshot | Maintained reader | Carries file IO, table metadata and encryption manager into manifest-list loading. |
| Native `ManifestListReader::load` | `iceberg-0.10.1/src/spec/manifest_list/reader.rs:52` | Snapshot list reference | Parsed native list | Reads/decrypts native bytes and parses with the metadata format version. |
| Native `ManifestFile::load_manifest` | `iceberg-0.10.1/src/spec/manifest_list/manifest_file.rs:181` | Manifest-list entry | Native manifest | Reads/decrypts the file and initializes inherited manifest-entry metadata. |
| Native `ManifestEntry::is_alive` | `iceberg-0.10.1/src/spec/manifest/entry.rs:70` | Entry status | Boolean | Includes Added/Existing, excludes Deleted. |
| Test `batch` / `values` | `src/storage.rs:458`, `466` | Nullable integer fixture / pin | Arrow batch / sorted values | Checks duplicate-aware values without promising physical ordering. |

## Data flow and invariants

1. **Native metadata determines required files.** The table supplies exact metadata
   location and retained snapshots (`279-291`), maintained manifest readers resolve
   file descriptors (`292-306`), and path sets remove repeated physical references.
   Directory leftovers no longer become release dependencies merely by existing.
   F1 identifies a remaining reference class outside this traversal.
2. **Sharing retains byte identity and owner.** For paths present in `shared`, the
   helper verifies the supplied digest/size and clones the original reference
   (`389-391`). It does not rewrite the file or relabel its owner. F2 identifies
   ambiguity when callers provide contradictory declarations.
3. **Uninherited external files refuse.** For other paths, canonical path inclusion
   in the candidate table root and direct symlink refusal precede owner assignment
   (`393-397`). This is local path admission, not proof of a durable owner token.
   The higher-level engine's existing protected directory reservation remains the
   authority for fresh writes.
4. **Prepared membership is checked.** Current live native file counts must equal
   the supplied prepared row count (`363-373`). Native delete files are refused for
   current candidates (`309-313`), so summing live data-file counts is meaningful
   within this supported profile. Independent decode later checks that count again
   (`src/engine.rs:971-983`).
5. **No incomplete pin succeeds silently.** Verification hashes listed objects
   first, opens exact metadata and compares discovered dependencies with listed
   paths (`src/engine.rs:968-970`; `src/storage.rs:406-424`). Removing a shared data
   object from the pin is therefore refused even if native reading could find it
   on disk. That is the central useful correctness improvement over directory
   inventory.
6. **No publication or removal authority is introduced.** The only production
   caller still prepares a full fresh table. The new storage helper does not make
   the cross-owner lifecycle protocol complete; no deletion is enabled.

## Test behavior and edge cases

The probe `candidate_snapshots_share_unchanged_files_with_complete_owned_inventories`
(`src/storage.rs:486`) is expected, by static trace, to establish the following
bounded behavior if its assertions pass:

| Scenario | Assertions in initial diff | Expected behavior |
| --- | --- | --- |
| Original source has repeated values and a null | `497-529`, `603-606` | Source values remain `[null, 1, 1, 2]`. |
| Candidate uses a replacement plus an unchanged file | `543-554`, `607-610` | Candidate values are `[null, 2, 3, 3]`. |
| Missing inherited owner | `555-561` | Pinning refuses `unprotected_reference`. |
| Preserved inherited owner | `562-575` | The shared file retains owner `source`, not `derived`. |
| No physical rewrite of shared file | `576-580` | Digest, byte size and modification time stay unchanged. |
| Replaced file excluded from candidate inventory | `581-592` | Candidate does not retain the source's replaced data file as a dependency. |
| Producing catalog unavailable | `593-610` | Both pins verify and native readers decode values without the catalog. |
| Pin omits shared native data dependency | `612-622` | Verification refuses `incomplete_inventory`. |

The test does not cover conflicting owner declarations, statistics references,
multiple retained snapshots within one metadata file, current delete-file refusal,
duplicate live file descriptors or metadata-capacity exhaustion. Those are concrete
limits of this probe; it is not evidence for incremental selection or retention.
F1/F2 request targeted boundary coverage before downstream callers rely on those
properties. Existing integration verification paths now transitively exercise
`check_inventory` for the already supported full-run fixtures.

## Exploration notes

- **H1:** Directory scanning previously omitted externally shared native files.
  **Confirmed and addressed:** The new path follows manifest references instead.
- **H2:** Cloning a native descriptor can preserve unchanged bytes and old reads.
  **Supported by static test trace:** The new probe asserts file identity plus both
  source and candidate values with the catalog hidden; execution was not repeated.
- **H3:** Every native object reference enters ownership admission.
  **Refined:** Snapshot/manifest/data references do; native statistics references
  do not yet (F1). Prior metadata-log history is intentionally excluded.
- **H4:** Shared ownership is invariant under equivalent argument ordering.
  **Refuted for conflicting declarations:** Map construction silently chooses the
  last entry (F2).

## Closure review

The revised diff was inspected after the implementing agent addressed both findings.
These references identify the updated source; earlier references preserve the
initial review state.

| Finding / change | Disposition and evidence |
| --- | --- |
| F1: statistics reference closure | **Closed.** Both maintained statistics iterators contribute paths before native manifest traversal (`src/storage.rs:285-290`). Those paths pass through the same inherited-owner or owned-root admission as data files (`427-436`) and the same completeness check (`445-461`). The probe attaches a statistics reference using the native transaction action (`671-686`), checks absent-owner refusal (`687-699`), preserved inherited identity (`700-705`) and missing-reference refusal (`706-715`). |
| F2: ambiguous inherited identity | **Closed.** Map construction compares any previous entry against the complete `ObjectRef` and returns `conflicting_owner` on a difference (`405-415`). Identical duplicates collapse safely. The probe supplies the same data path under a different owner and requires refusal (`619-634`). The comparison also covers conflicting digest or size. |
| Shared manifest traversal | **Statically sound for this profile.** Current snapshot is traversed first (`293-300`), so deduplication cannot suppress current data-file discovery. A repeated manifest in the current list refuses; the same immutable manifest referenced by a historical snapshot is skipped after its paths were already collected (`309-318`). The probe now performs successive native source appends (`554-570`) and verifies the inventory under a limit based on unique metadata files (`572-586`). |
| Row counts and final admission | **Improved.** Native row counts use checked addition (`389-398`); a final inventory-size check covers empty or metadata-only cases too (`354`). |

The statistics fixture intentionally uses opaque bytes and tests reference closure
only (`666-669`); it is not evidence that a Puffin statistics decoder works. The
partition-statistics branch uses the same direct path insertion, but the probe
constructs a regular statistics file only. Current delete-file and duplicate-live
descriptor refusal remain unexecuted branches in this review. The current storage
writer and positive fixture produce materialized data files without deletes.

The reviewer did not run the gate. The implementing agent is rerunning it after
these changes. Static inspection supports the targeted assertions and the closure
of both findings; execution results must be recorded by that gate separately.

## Conclusion

**VERDICT: APPROVE** — scoped to this native file-sharing primitive.

- Exact native dependency discovery, inherited identity preservation, omission
  refusal and catalog-free old/new reads have coherent implementation and test
  traces. Both actionable review findings are closed.
- **Coverage of changed paths: ADEQUATE** for the identified fixes and the bounded
  shared-file fixture, with unsupported native forms and broader lifecycle behavior
  still outside the qualification established here.
- **Confidence: HIGH** in static control-flow closure. This is not an independent
  execution certificate or approval of incremental application selection,
  cross-owner protection acquisition, destructive retention or restore.
- The changes use maintained native readers and small local validation checks;
  they introduce no separate catalog or coordination subsystem.
