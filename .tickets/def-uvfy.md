---
id: def-uvfy
status: in_progress
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

