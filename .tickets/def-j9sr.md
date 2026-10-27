---
id: def-j9sr
status: in_progress
deps: []
links: []
created: 2026-10-27T11:23:27Z
type: task
priority: 2
assignee: mikewolfd
tags: [engine-first, C7]
---
# C7: declarative SQL job CLI and library runner

## Acceptance Criteria

One typed job spec declares inputs, SQL steps, exports and scratch; library and CLI execute the same path and return machine-readable results. Local fixture runs SQL service/export contracts without opening lifecycle workspace. Basin M2 workload remains owner-gated by M1.

