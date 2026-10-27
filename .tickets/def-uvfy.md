---
id: def-uvfy
status: closed
deps: []
links: []
created: 2026-10-27T18:42:49Z
type: task
priority: 1
assignee: mikewolfd
tags: [engine-first, C5]
---
# C5: exact pinned metadata GET and remote SQL jobs

## Acceptance Criteria

Remote job startup shares native object I/O and the existing job executor without lifecycle. Pinned metadata uses one bounded GET with exact size/digest/snapshot checks and no HEAD; local injected FileIO checks are identical across builds. Fake/local request-count and CLI/library checks pass. Live Basin remains owner gated.


## Notes

**2026-10-27T19:08:33Z**

C5 read/startup slice completedb9ea2ec:35 object tests,44SQL tests,15Python client checks, Clippy pass. CODE/ARCH APPROVE. Explicit RemoteStorage enters same JobSpec/Budget; oneGET fullsize/range/digest/native pin checks, no HEAD; local/injected checks identical across builds. Empty-table metadata and constant-SQL fixtures establish this seam, not populated remote/M1/RSS acceptance. SinglePUT release upload is a separate C5 slice, still outstanding.
