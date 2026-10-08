# Local implementation results

The implemented local path passes the integration gate and independent retained-data
checks. These results qualify selected mechanisms and inputs; the full lifecycle
and 10-million-Comments target remain unfinished.

## Correctness and recovery

[`check.log`](check.log) records the pre-review formatting, Clippy and integration-test
gate. The later [foundation review gate](../../docs/reviews/foundation-check.log)
covers the reviewed fixes. The tests cover actual catalog and Parquet operations, abrupt CLI process
termination after durable handoffs, recovery without the original input, stale
dependency and coverage refusal, concurrent retry reconciliation, historical reads,
catalog-free verification, null/empty/duplicate distinctions, HTTP responses and
same-size corruption. An oversized-output refusal retains admitted bytes and
resumes under an explicitly enlarged resource profile.

A failed development test found that DataFusion 53.1.0 implements `EXCEPT ALL`
through an anti join, which missed the removal of one matched duplicate. The
comparison now uses grouped signed occurrence counts through maintained query
operators. User queries requesting `EXCEPT ALL` or `INTERSECT ALL` receive an
explicit refusal. The duplicate-removal regression is retained in the integration
suite. This is a qualified adapter restriction, not a claim that all SQL is verified.

## Metadata history: a measured reason for a cache

The [original history protocol](history-protocol.md) fixed the data and native
snapshot while retaining more operations. The
[baseline](history-20261007T213458/results.json) exceeded its 3-second unchanged-run
budget at 50 retained operations. The
[cache intervention](cache-protocol.md) was recorded before its measurement.

| Retained operations | Baseline unchanged run | With exact-snapshot cache |
| --- | ---: | ---: |
| 10 | 1.240 s | 0.430 s |
| 50 | 5.503 s | 1.229 s |

At 50 operations, the complete unchanged run improved by approximately 78% and
its peak RSS was 68.8 MiB. The latest lookup remained approximately 0.60 seconds.
The [intervention results](history-20261007T214138/results.json) preserved the
original native data and source pins throughout. All declared local probe budgets
passed. The first-operation measurement was 0.825 seconds versus 0.766 seconds in
the baseline; these single observations do not establish a fresh-run improvement.

The cache stores one exact immutable snapshot in bounded memory. It avoids reading
the same lifecycle records repeatedly during an operation. It neither supplies a
mutable authority pointer nor bypasses native guarded commits. Restarted commands
still load the selected snapshot from the catalog.

Verdict: **bounded improvement** for complete unchanged runs in this history range.
The baseline is historical, OS caches were not cleared, and background workstation
load was not controlled. The cache does not reduce retained metadata: at 50 operations
the workspace still held about 26.3 MiB, including about 18.3 MiB of metadata JSON.
Larger histories, fragment counts, evidence depth and maintenance need separate
qualification. This is a partial Q7 probe, not a Q7 pass.

## Retained-data reuse

The [reuse protocol](protocol.md) compares fresh workspaces, new unchanged operations
and exact completed-operation retries. It checks the locally retained Bill Actions
sample and its nested receipt table against their original SHA-256 member manifest.
A separate PyArrow reader checks exact types, nulls, values and multiplicity against
the source. The source objects are not modified or downloaded again.

The initial [raw run](run-20261007T212243/results.json) passed those checks and showed
bounded reuse improvements before the cache intervention. Its artifacts remain
retained, including the exact binary digest. Post-cache, pre-review measurements are recorded
separately below; earlier observations are not overwritten.

The [post-cache run](run-20261007T214343/results.json) passed every retained-data
check in all preregistered repetitions. Medians include Rust's independent native
readback; the external PyArrow comparison is recorded separately.

| Input | Fresh run | New unchanged operation | Exact completed retry |
| --- | ---: | ---: | ---: |
| Bill Actions sample | 0.600 s | 0.269 s | 0.085 s |
| Nested receipts | 2.813 s | 0.835 s | 0.087 s |

The new unchanged operations improved median time by approximately 55% and 70%,
respectively, and were faster in every paired repetition. They retained the original
native snapshots and input objects. Exact retries returned the same release and
left the retained-file inventory unchanged. Peak measured process memory across
the measured fresh runs was approximately 162 MiB. Verdict: **bounded improvement**
under the reuse protocol. These measurements use the debug build with test hooks
compiled in; the hooks are inactive during the probe. The binary digest, dependency
lock, input identity, raw outputs, timing and memory records identify each run.

No benchmark here includes 10 million Comments, docket dependency refresh, flat
public exports, value-level evidence, R2, destructive maintenance or restore. Local
worker-loss tests do not establish power-loss durability or remote fencing.

The foundation review subsequently tightened metadata replay admission, native reader
concurrency, blocking I/O admission and query repeatability checks. The timings
above describe their recorded binaries, before those fixes; they are not a
performance claim for the reviewed commit.
