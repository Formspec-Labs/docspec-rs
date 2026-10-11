# Current Rust versus original DocSpec

The current optimized Rust engine was faster on these bounded native-table
workloads. For 102,400 retained Comments, median admission plus native read fell
from **2.717 seconds to 0.548 seconds**, a **4.95× speedup**. The complete worker,
including startup, retry, another pinned read and exact independent comparisons,
fell from **3.479 seconds to 0.944 seconds**. Rust used about half the observed
peak memory but retained **23.1% more logical disk space** for Comments.

These results compare the actual implementations and configurations. They do not
isolate Rust versus Python as languages, and they are not a full 10-million-row
Comments lifecycle comparison.

## What ran

Three fresh paired repetitions per case, sequentially, with alternating arm order.
Both arms consumed exactly the same prepared Parquet bytes and returned the same
columns, types, nullability and every value. Both returned the same immutable state
on unchanged retry. All primary runs passed; no result was discarded.

- **Original:** Python DocSpec 0.12.4, commit
  `168079e53474c403030d93a70c8a5f6093574f9d`; locked dependencies installed offline.
  DuckDB 1.5.5, PyArrow 25.0.1, PyIceberg 0.12.0, SpicyDocs 0.54.0.
- **Current Rust:** commit `9d01704983a1400ef80c40ae3341ee60a61af291`, freshly built
  with `cargo build --release --locked --bin docspec` and
  `CARGO_PROFILE_RELEASE_DEBUG=0`; local features, Rust 1.94.0. The build took
  5m 52s, separately from measurements. Exact binary/lock identities are in
  [current-preparation.json](current-preparation.json).
- **Cases:** retained Bill Actions, 20,000 rows; and 25 deterministic, evenly spaced
  row groups from the retained 10M Comments selection, totaling 102,400 rows.
  All selected original values were preserved. Both arms received an added unique
  benchmark key, preserving duplicate user rows while supporting Python's keyed
  states. This is a selected sample, not a representative population estimate.
- **Machine/configuration:** shared 14-core, 48 GiB workstation; uncleared OS caches;
  fresh application workspaces. Both engines received a 6 GiB memory setting.
  Python retains its required one-thread native writer; Rust uses its reference
  12 compute threads. Auxiliary Arrow/BLAS threads were limited to one.

The source wrapper is an explicitly experimental single-table Rulespec generation.
It excludes the original family's ETL receipts. Python uses
`CoreWorkspace.admit_generation` followed by `open_state().table()`. Rust uses
`run` with `SELECT * FROM input`, then `read`. Both stream the complete selected
columns to Arrow IPC. Python also maintains row membership and identity indexes;
Rust records physical occurrences and writes native output. Those differences
are part of the real persistent-table implementations being compared.

## Timing

Medians below; parentheses show the observed minimum–maximum. Admission/read
excludes interpreter imports and the independent oracle. Python retry uses its
live workspace, while each Rust call starts a fresh CLI process. Reopen creates a
new Python workspace handle or Rust command; it does not clear OS caches.

### 102,400 Comments

| Measure | Original DocSpec | Current Rust | Median comparison |
|---|---:|---:|---:|
| Admission + first native read | 2.717 s (2.659–2.810) | 0.548 s (0.545–0.556) | **4.95× faster** |
| Same-request retry | 40.9 ms (33.0–46.5) | 17.1 ms (17.0–17.1) | **2.40× faster** |
| Reopen pinned result + native read | 269.3 ms (253.9–373.5) | 113.7 ms (106.9–127.7) | **2.37× faster** |
| Entire worker, including imports and exact checks | 3.479 s (3.465–3.729) | 0.944 s (0.936–0.968) | **3.68× faster** |

### 20,000 Bill Actions

| Measure | Original DocSpec | Current Rust | Median comparison |
|---|---:|---:|---:|
| Admission + first native read | 0.804 s (0.701–0.937) | 0.259 s (0.249–1.514) | **3.10× faster** |
| Same-request retry | 17.1 ms (17.0–17.9) | 16.1 ms (15.2–17.0) | Approximately equal |
| Reopen pinned result + native read | 74.9 ms (73.8–79.3) | 15.1 ms (14.8–16.5) | **4.95× faster** |
| Entire worker, including imports and exact checks | 1.308 s (1.082–1.775) | 0.369 s (0.355–1.631) | **3.55× faster** |

The first current Rust Bill Actions admission/read took 1.514 seconds and lost
that pair. It remains in the range and median calculation. The cause was not
isolated; do not present every first-use run as faster. Comments won every pair.
Small millisecond differences are not treated as meaningful wins.

## Memory and disk

These are medians across repetitions. Memory is the largest observed process-tree
sample or operating-system process high-water mark during the **whole worker,
including the oracle**. It is not an isolated engine memory measurement or a
continuously observed aggregate peak.

| Measure | Original DocSpec | Current Rust |
|---|---:|---:|
| Comments observed peak memory | 1.98 GiB | 1.00 GiB |
| Comments retained engine workspace | 29.59 MB | 36.42 MB |
| Comments retention / shared 18.25 MB input | 1.62× | 2.00× |
| Comments peak observed run files, including Arrow handoff | 209.28 MB | 216.32 MB |
| Bill Actions observed peak memory | 338.5 MiB | 82.8 MiB |
| Bill Actions retained engine workspace | 2.57 MB | 0.62 MB |

For Comments, Rust retains the input and its rewritten native table; the original
registers the admitted Parquet without rewriting it. The measured extra Rust
retention is 6.83 MB, or 23.1%. For the small Bill Actions table, Python's membership
and identity storage dominate, and Rust retains substantially less.

Retained workspace figures include data, catalog, metadata and lifecycle state;
they exclude the shared prepared input outside the workspace, build/environment
files and measurement logs. Successfully compared Arrow handoffs were deleted
immediately, while native states and input files remain available locally.
Logical bytes, allocated blocks and whole-volume free space are recorded
separately. Hard links/APFS allocation do not establish unique physical extents.
One-second RSS/free-space and five-second directory sampling can miss short peaks;
explicit handoff-boundary measurements supplement the latter. Physical device I/O,
cache-hit rates and exact scratch peaks were not measured.

The initial low-disk constraint was resolved by removing only inactive rebuildable
Rust dependency/example build files, after checking for open holders. The existing
executables, datasets and prior qualification evidence were retained. The
[cleanup receipt](build-cache-cleanup.json) distinguishes removed logical bytes
from the measured free-space change. No benchmark crossed its disk/memory/time
stop bound. Shared workstation load and the recent build limit generalization;
no Cargo/rustc process was present in either collection's starting process snapshot.

## Meaning and limits

This supports the current Rust engine for the measured persistent-table admission,
read and retry paths. It also shows why storage belongs in the performance decision:
the faster Comments path keeps a larger retained representation.

It does not benchmark joins, summaries, schema additions, incremental corrections,
long histories, export/publication, crash recovery, object storage or the full
10-million-row path against the original. Do not extrapolate these small-case
ratios to that larger lifecycle or infer a language-level speedup. The existing
Rust-only full Comments acceptance remains separate evidence.

An initial diagnostic series used the older qualified optimized Rust `e40be018`
and the current debug binary. Those observations are retained in
[results.json](results.json), but none enters the primary figures above. The
[protocol](protocol.md) preserves the original decision and the later, explicitly
registered current-release extension. Python controls were rerun after the build
to avoid comparing results across the changed cache/background conditions.

## Evidence and reproduction

- [Primary raw results](current-results.json), [derived summary](summary.json),
  [summary calculation](summarize.py), [source hashes](current-execution-sources.json).
- [Input preparation and exact subsets](preparation.json), [preparation script](prepare.py),
  [current build capture](prepare-current.py), [build log](current-release-build.stderr.log).
- [Worker](worker.py), [sequential current runner](run-current.py),
  [resource monitor](monitor.py), [collection environment](current-collection-environment.json).
- [Raw command logs, pins, checks and samples](raw-evidence.tar.gz).
- [Independent review](REVIEW.md).

The primary inputs/native workspaces are retained under this experiment's ignored
`inputs/` and `current-runs/` directories; diagnostic workspaces are under `runs/`.
The archive retains their bounded command/selection/check/measurement evidence,
not duplicate dataset payloads. A fresh collection needs a new output directory;
the runners refuse to overwrite existing series. No production code changed.
