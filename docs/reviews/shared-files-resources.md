# Shared-file inventory review: resources and native snapshot fidelity

**Current disposition: APPROVE this native sharing checkpoint.** The original
findings remain below; the final closure records repairs made during the review.

Static review on 2026-10-07 of the uncommitted changes relative to the foundation
HEAD. Scope: `src/storage.rs` and its changed callers in `src/engine.rs`. This
review applies the semi-formal-code-review skill and relevant Rust guidelines.
The reviewer did not run tests, benchmarks, or change implementation files.

## Findings

### SF1 — Keep inventory admission inside the detached blocking worker

**Severity: WARNING. Category: resource correctness. Status: resolved during review.**

The first version of `pin` used only `inventory.paths` in its `move` closure.
Under the crate's Rust 2024 edition, disjoint capture moves that field rather than
the complete `SnapshotFiles`. Its `_reservation` therefore remained in the calling
future: canceling that future could release inventory admission while the detached
hashing worker still held the path inventory. `Budget::blocking` retains its own
one-MiB buffer and job permits (`src/budget.rs:73–95`), but those do not account for
the separately admitted inventory.

The inspected repair explicitly moves the entire value into the closure with
`let inventory = inventory;` (`src/storage.rs:382–386`). This keeps the inventory
permit with the worker until it exits, including after caller cancellation.
No additional scheduler or resource type is needed. The existing blocking-worker
cancellation test exercises the outer worker's lifetime; there is not a separate
runtime assertion for this new inventory capture. Closure ownership is sufficient
to establish this specific static property.

### SF2 — Repeated snapshot references reload and overcharge identical manifests

**Severity: WARNING. Category: performance / capacity. Status: open in initial inspection.**

Location in the inspected version: `src/storage.rs:286–304`.

The inventory visits every snapshot retained in native metadata. For each manifest
reference, it performs a metadata request, adds that file's size to
`metadata_bytes`, and loads and decodes the manifest again, even if the same path
has already been inserted in `paths`. Native append snapshots commonly retain
previous immutable manifest files, so a sequence of appends revisits the same
files across retained snapshots. Deduplicating the *final* path set does not avoid
these reads or charges.

The pinned loader performs real file reads and Avro parsing on each invocation:
`iceberg-0.10.1/src/spec/manifest_list/manifest_file.rs:181–198`; it does not supply
an implicit cache. Consequently, work grows with repeated snapshot references,
and `metadata_capacity` can refuse a table whose distinct required metadata still
fits the declared allowance. All release verification now traverses this path
through `Engine::verify_table` (`src/engine.rs:967–974`), so the cost affects reads
as well as preparation.

**Suggested fix:** traverse the current snapshot first to collect its exact live
data descriptors, then skip immutable manifest paths already traversed when
visiting historical snapshots. Count each distinct manifest's bytes once. Preserve
duplicate-file detection within the current snapshot rather than allowing a
repeated manifest there to hide duplicated membership. Keep complete historical
closure; no durable cache is required. Add a retained-history case that shares
manifests and checks current membership plus unique-file capacity accounting.

## Patch summary

The patch replaces directory enumeration with native snapshot traversal. Its
practical benefit is that a candidate table can reference an unchanged data file
owned outside its own directory while recording that file's original identity and
owner. It also makes verification reject a pin that omits a live referenced file.

Production processing still creates a fresh table and calls `pin` with an empty
inherited-file list (`src/engine.rs:552–555`). The shared-file path is exercised by
a real native test, not yet exposed as an application update operation. This is a
storage primitive, not completed incremental refresh, protection acquisition,
retention, or restore support.

## Function trace

| Function / type | File:line in inspected version | Input → output | Verified behavior |
| --- | --- | --- | --- |
| `SnapshotFiles` | `src/storage.rs:273` | Native metadata → paths, current data descriptors, permit | Keeps inventory admission attached to the descriptor until ownership is explicitly moved or dropped. |
| `snapshot_files` | `src/storage.rs:279` | Native table + shared budget → inventory | Visits retained snapshots, manifest lists, manifests and alive entries. Includes historical object paths; returns only current live data descriptors. Current native delete files and duplicate live data paths refuse. |
| `check_inventory_size` | `src/storage.rs:337` | Path count, metadata bytes, limits → admission/refusal | Bounds encoded metadata and approximate object count. It does not establish whole-process allocation bounds for native Avro decoding. |
| `pin` | `src/storage.rs:356` | Owner, table, expected row count, budget, inherited object references → exact pin | Checks current native row counts with checked addition; hashes new owned files; verifies inherited identities and preserves their owner; rejects unowned outside paths. |
| `check_inventory` | `src/storage.rs:404` | Pin + static table + budget → completeness result | Requires exact metadata object in the pin and every traversed native path in its object inventory. Extra retained objects are allowed. |
| `open_pin` | `src/storage.rs:429` | Exact metadata identity → static native table | Existing behavior verifies metadata digest, snapshot and schema without the producing catalog. |
| `Engine::run_internal` | `src/engine.rs:552–565` | Streamed fresh result → processing phase | Writes native files, commits snapshot, awaits new inventory-based pinning, and only then records completion. |
| `verify_table` | `src/engine.rs:967` | Pin + shared budget → verification | Hashes pinned objects, opens the static table, validates closure, then decodes and counts current rows. |
| `Budget::blocking` | `src/budget.rs:73` | Owned closure → result | Uses maintained Tokio blocking work; holds job and buffer permits inside the worker across caller cancellation. |
| Test `batch`, `values` | `src/storage.rs:458`, `466` | Small typed values / pin → fixture / sorted values | Preserves nulls and duplicate integers; reads through the native reader and sorts only for comparison. |

Important upstream implementations were read from the locally installed pinned
dependency under `/Users/mikewolfd/.cargo/registry/src/index.crates.io-1949cf8c6b5b557f/`:

- `iceberg-0.10.1/src/table.rs:305–311` constructs the maintained manifest-list
  reader with exact snapshot and table metadata.
- `iceberg-0.10.1/src/spec/manifest_list/reader.rs:52–75` reads and parses the
  referenced list.
- `iceberg-0.10.1/src/spec/manifest_list/manifest_file.rs:181–198` reads and parses
  each manifest and initializes its inherited entry values.

## Data flow and invariants

| State | Creation, modification and use | Invariant / limitation |
| --- | --- | --- |
| Native object closure | Exact metadata location → all retained snapshot lists → manifest paths → alive data/delete paths (`src/storage.rs:281–328`) | Referenced shared files can live outside the candidate directory; unrelated directory files are no longer accidentally included. |
| Current file descriptors | Only entries whose snapshot ID matches current head are inserted (`src/storage.rs:308–324`) | Prepared row count refers to current membership; historical paths do not inflate the selected rows. |
| Inventory admission | Reserved before native traversal (`src/storage.rs:280`), returned in `SnapshotFiles`, moved into hashing worker (`:383`) | The repaired move keeps ownership with actual retained memory through cancellation. |
| Shared ownership identities | Caller-provided `ObjectRef` list → keyed map → digest verification → inherited reference (`src/storage.rs:372–389`) | Preserves original digest, size, path and owner. This does not itself create or validate a durable cross-dataset protection token. |
| Newly owned files | Canonicalized table root and referenced path → containment/leaf-symlink check → new digest (`src/storage.rs:384–395`) | Uninherited outside files refuse. Runtime-created table directories remain protected by the existing application phase flow. |
| Verified pin | Native-required path set compared with pinned references (`src/storage.rs:404–422`) | Missing referenced files refuse even if remaining pinned objects and current row counts would otherwise be readable. |

Review hypotheses were: directory replacement would correctly discover external
shared files (confirmed by native traversal and the test); parsed-inventory
admission would survive cancellation (initially refuted, then closed by SF1); and
deduplicated paths would imply deduplicated metadata reads (refuted by SF2).

## Tests and concrete edge cases

`candidate_snapshots_share_unchanged_files_with_complete_owned_inventories`
(`src/storage.rs:486` in the repaired capture version) constructs real source and
candidate Iceberg tables. It writes separate source files, reuses one native file
descriptor in the candidate, writes replacement values, and commits through the
maintained catalog. Assertions cover:

- Missing inherited ownership information refuses with `unprotected_reference`.
- The shared object's recorded owner remains the source owner.
- Shared bytes and modification time remain unchanged.
- The replaced source file is absent from the candidate's inventory.
- Hiding the catalog still allows both pinned snapshots to verify and read.
- Original and candidate values retain their expected duplicates and null.
- Removing the shared reference from a pin causes `incomplete_inventory`.

These assertions are consistent with the traced implementation; this reviewer
did not execute them. They establish a useful native sharing example, not complete
ownership or deletion qualification. The test has one snapshot per table, so it
does not exercise SF2, historical native deletion files, malformed native metadata,
or statistics-file retention.

The prior integration test for an empty result exercises the production pinning
path with no data snapshot (`tests/lifecycle.rs:459` in this working tree). The new
final inventory-size check also runs after the snapshot loop, so metadata-only
tables no longer bypass the entry admission check. Native row-count addition now
uses `checked_add`, avoiding a wrap or panic before comparison.

## Qualification and complexity assessment

This checkpoint adds a needed capability with maintained native APIs. Replacing
directory walks avoids treating orphan files as part of the selected result and
permits physical file reuse. No additional database, custom manifest format, or
generalized execution system is introduced.

The test establishes no-rewrite sharing by unchanged file identity and modification
time. It does **not** establish constant-time incremental work: `pin` still hashes
every inherited file it includes (`src/storage.rs:387–389`), and verification reads
the required objects and current rows. Any later performance claim must measure
those full-data passes, native metadata requests, and retained-history growth.

The per-object estimate and encoded metadata cap are provisional admission checks,
as the code comment states (`src/storage.rs:341–343`). Native Avro allocations,
large file descriptors, and peak aggregate memory remain Q1 work. The inherited
owner label is evidence to carry into a future protection protocol; it is not proof
of reference acquisition winning a race against deletion. Cleanup remains disabled
and no such race is claimed qualified here.

## Initial conclusion

**VERDICT: REQUEST CHANGES** for SF2; SF1 is statically resolved.

The patch supplies the intended narrow native file-sharing primitive and improves
inventory fidelity. Current coverage is **ADEQUATE** for the single-snapshot
sharing example and **INSUFFICIENT** for repeated historical manifest references.
Confidence is **HIGH** in the ownership/capture trace and duplicate-loader finding.
No speed, RSS, hosted-support, incremental-update, or destructive-retention gate
is established by this static review.

## Final closure inspection

Subsequent source inspection closes both findings. These locations refer to the
repaired working tree, and supersede the initial dispositions without replacing
their evidence.

- **SF1 closed:** `pin` explicitly moves the whole `SnapshotFiles` into its
  blocking closure (`src/storage.rs:423–427`). Its admission permit therefore
  travels with the path inventory and survives cancellation of the waiting caller.
- **SF2 closed:** `snapshot_files` processes the current snapshot first
  (`src/storage.rs:293–300`). Its `visited_manifests` check runs before the metadata
  request, byte charge and decode (`:310–329`). Historical repeats are skipped,
  while current-snapshot repeats refuse `duplicate_manifest`. Duplicate live data
  paths remain checked separately (`:340–347`). Every retained manifest list is
  still visited, preserving the selected metadata's historical closure.

The expanded real-native test now makes two source commits
(`src/storage.rs:554–570`) and applies a tight metadata limit derived from unique
retained Avro objects (`:572–586`). It asserts the current inventory still contains
the two live files. This specifically covers repeated historical manifests and
their capacity accounting, in addition to the previously reviewed sharing case.

Two additional repairs were inspected for regressions:

- Conflicting inherited references for the same path now refuse before hashing
  (`src/storage.rs:405–415`), with the differing-owner test at `:619–634`. Exact
  duplicate identities remain harmless.
- Both native statistics collections contribute paths (`src/storage.rs:285–289`).
  A maintained `UpdateStatistics` commit introduces an external sidecar in the
  test (`:668–686`); missing inheritance refuses, explicit inheritance retains the
  reference, and dropping it from the pin produces `incomplete_inventory`
  (`:687–715`). The test correctly labels the sidecar as opaque and makes no claim
  to validate the Puffin format.

**VERDICT: APPROVE**, subject to the implementation owner's executable gate.
Coverage of the repaired ownership and repeated-manifest branches is **ADEQUATE**
by static trace; confidence is **HIGH**. No actionable defect remains in the
reviewed checkpoint. This does not close aggregate RSS, retained-history scale,
durable cross-owner protection, public publication, or full lifecycle qualification.
