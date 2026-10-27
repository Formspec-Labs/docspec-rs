---
id: def-3evz
status: in_progress
deps: []
links: []
created: 2026-10-27T16:12:34Z
type: task
priority: 1
assignee: mikewolfd
tags: [engine-first, C4]
---
# C4: bounded generic connected components in SQL jobs

## Acceptance Criteria

SQL jobs stream two named string endpoint columns through shared union-find, emit node/component with bytewise-minimum labels, enforce node and byte ceilings before growth, and retain or spill through existing named-step storage. Deterministic, invalid-input and capacity fixtures pass.

