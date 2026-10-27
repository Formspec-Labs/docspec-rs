---
id: def-3evz
status: closed
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


## Notes

**2026-10-27T16:46:01Z**

Generic component-step acceptance met: all four graph fixtures pass, including deterministic labels across order/threads, exact caps, null/type refusal, 33-node rehash and populated-growth refusal with same-job reuse and final scratch cleanup. The 30-test SQL group and all-target/all-feature Clippy pass before the added coverage-only fixture. ARCH APPROVE; CODE coverage concern fixed in the final fixture commit. M2 real-graph/heap acceptance and phase-2 dense IDs remain separate plan work.
