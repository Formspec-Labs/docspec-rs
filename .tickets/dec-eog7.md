---
id: dec-eog7
status: in_progress
deps: []
links: []
created: 2026-10-28T18:30:15Z
type: bug
priority: 1
assignee: mikewolfd
---
# Bound retained SQL memory so native operators can execute

## Acceptance Criteria

192 MiB raw-shaped retention/window/aggregate fixtures pass unchanged limits; retained bytes have separate documented allowance; spill reservations scale; full engine gate and reachable dependent tests pass; independent review resolved.

