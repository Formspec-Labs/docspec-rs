---
id: def-z1s2
status: closed
deps: []
links: []
created: 2026-10-27T16:44:00Z
type: task
priority: 1
assignee: mikewolfd
tags: [engine-first, C2]
---
# C2: guarded append retries and checked native purge

## Acceptance Criteria

The generic table service recomputes a work list after conflict, makes at most three conflict retries, reuses completed files only for identical work/schema/spec/UUID, and checks exact UUID plus metadata before one purge attempt. Local catalog races, reuse, no-op and exhausted-retry fixtures pass; live M1 remains owner gated.


## Notes

**2026-10-27T17:10:14Z**

Generic library acceptance met at bb1e8f0: five local catalog fixtures pass with both ordinary remote-only and all features; one transient-purge request-count fixture and all eleven local probe tests pass; all-target/all-feature Clippy passes. CODE and ARCH APPROVE. Probe ingest now reuses the helper. M1 live format/cap qualification and C7 JobSpec table-write binding stay open. Historical contract staging note: 7f12fff nested the purge fixture; bb1e8f0 corrected top-level enrollment, and the final correctly enrolled test passed. No history rewritten.
