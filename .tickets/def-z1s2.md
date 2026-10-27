---
id: def-z1s2
status: in_progress
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

