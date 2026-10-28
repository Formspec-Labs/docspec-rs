---
id: def-h6g9
status: in_progress
deps: []
links: []
created: 2026-10-28T00:25:07Z
type: bug
priority: 1
assignee: mikewolfd
---
# Bound Parquet scan metadata and decode memory under the SQL job budget

C6: raw immutable reproduction codex-coord/shared/raw-prior-repro-fdb9242c contains exact jobs, 0c5545a/48636e5 configs, limits, input lists and manifest. Prior-only original failed in188.26s/maxRSS24471601152B under configured14GiB (7GiB DataFusion pool plus7GiB custom admission),3GiB spill. Explicit materialized prior_scalars486 prototype completed207.47s/maxRSS23.64GB/64761groups, so view compaction does not establish a fix. Investigate heap/allocation profile on original486 control:218332850B footer, cache50MiB oversize refusal/repeated decode,3independent parquet scans underUnion, row-group decode outsidepool are hypotheses. No heap profile collected yet. Owner defers fix until E2 first release unless narrowD1reproduces. Done when real reproduction and regression test show RSS within configured admission plus measured small fixed overhead, without weakening correctness or increasing limits.


## Notes

**2026-10-28T02:55:52Z**

Narrow official audit8891171 peaked7105298432B under unchanged14GiB configured memory and refused64MiB function output in63.22s without panic; no new unaccounted narrow-path excess identified. Isolated native Parquet dictionary/view regression: >2GiB logical text, <2MiB stored/shared input, exact2048 narrowresults, peakRSS52822016B versus128MiB configured+64MiB fixed overhead. Wide24GB reproduction remains deferred by owner until firstE2release; this test does not qualify wide path.

**2026-10-28T03:22:03Z**

Final code gate passes with isolated process-memory test registered as explicit target. Current narrow audit remains within14GiB; no further narrow-path accounting defect reproduced. The historical wide job remains explicitly owner-deferred until first E2 release; do not close from the52.8MB narrow regression or partial official audit.

**2026-10-28T12:07:40Z**

Run4 view hardening: measured raw caption width averages463/max593bytes versus670MB sort reservation. Candidate unused StringView backing, not allocation attribution. Contract0702c9c predates behavior; seven alignment tests now PASS including retained admission, null headers, and >2GiB logical sharing. Add native pre-sort reproduction before qualifying operator seam. Existing historical wide-memory done-when remains open.
